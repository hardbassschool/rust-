use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

use crate::engine::{
    BoxFuture, DeduplicationStore, LedgerSettlementTool, SettlementEngine, SettlementError,
    TradeClaim,
};
use crate::planner::{
    ExecutionPlan, Planner, ProcessingGoal, ProcessingGraph, ProcessingStage, StageDecision,
};
use crate::policy::{GovernancePolicy, PolicyAction, ProcessingCriteria};
use crate::tools::{
    MessageEnvelope, MessageMetadata, ObserverTool, OffsetCommitTool, PayloadDecoder, ToolRegistry,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessingOutcome {
    Applied,
    Duplicate,
    InvalidPayload,
    EmptyPayload,
    PolicyRejected { reason: String },
    ProcessingFailed { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessingReport {
    pub metadata: MessageMetadata,
    pub outcome: ProcessingOutcome,
    pub detail: String,
    pub committed: bool,
    pub executed_stages: Vec<ProcessingStage>,
    pub plan: ExecutionPlan,
}

pub trait EnvelopeProcessor: Send + Sync {
    fn process_envelope<'a>(
        &'a self,
        envelope: MessageEnvelope,
    ) -> BoxFuture<'a, Result<ProcessingReport>>;
}

pub struct SettlementOrchestrator<P, D, L, Dec, Comm, Obs, Pol> {
    planner: Arc<P>,
    engine: Arc<SettlementEngine<D, L>>,
    tools: ToolRegistry<Dec, Comm, Obs>,
    policy: Arc<Pol>,
    criteria: ProcessingCriteria,
    concurrency_limit: Arc<Semaphore>,
}

impl<P, D, L, Dec, Comm, Obs, Pol> SettlementOrchestrator<P, D, L, Dec, Comm, Obs, Pol>
where
    P: Planner,
    D: DeduplicationStore,
    L: LedgerSettlementTool,
    Dec: PayloadDecoder,
    Comm: OffsetCommitTool,
    Obs: ObserverTool,
    Pol: GovernancePolicy,
{
    pub fn new(
        planner: P,
        engine: SettlementEngine<D, L>,
        tools: ToolRegistry<Dec, Comm, Obs>,
        policy: Pol,
        max_in_flight: usize,
    ) -> Self {
        Self {
            planner: Arc::new(planner),
            engine: Arc::new(engine),
            tools,
            policy: Arc::new(policy),
            criteria: ProcessingCriteria::default(),
            concurrency_limit: Arc::new(Semaphore::new(max_in_flight.max(1))),
        }
    }

    pub async fn process_message(&self, envelope: MessageEnvelope) -> Result<ProcessingReport> {
        let _permit = self
            .concurrency_limit
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("Settlement orchestrator is shutting down"))?;

        let plan = self
            .planner
            .build_plan(ProcessingGoal::HandleSettlementEvent);
        let mut executed_stages = vec![ProcessingStage::Ingest];
        self.tools.observer.observe(
            "ingest",
            &envelope.metadata,
            "Received settlement event envelope",
        );

        let validate_stage = Self::advance_stage(
            ProcessingStage::Ingest,
            StageDecision::Continue,
            &mut executed_stages,
        );
        debug_assert_eq!(validate_stage, Some(ProcessingStage::Validate));

        let payload = match envelope.payload.as_deref() {
            Some(payload) => payload,
            None => {
                let detail = format!("{}", SettlementError::EmptyPayload);
                return self
                    .commit_and_report(
                        envelope.metadata,
                        plan,
                        executed_stages,
                        ProcessingStage::Validate,
                        StageDecision::CommitAndReport,
                        "validate",
                        ProcessingOutcome::EmptyPayload,
                        detail,
                    )
                    .await;
            }
        };

        let event = match self.tools.decoder.decode(payload) {
            Ok(event) => event,
            Err(err) => {
                error!(error = ?err, "Malformed payload received; skipping and committing offset.");
                let detail = err.to_string();
                return self
                    .commit_and_report(
                        envelope.metadata,
                        plan,
                        executed_stages,
                        ProcessingStage::Validate,
                        StageDecision::CommitAndReport,
                        "validate",
                        ProcessingOutcome::InvalidPayload,
                        detail,
                    )
                    .await;
            }
        };

        match self.policy.evaluate(&event) {
            PolicyAction::Continue => {}
            PolicyAction::SkipAndCommit { reason } => {
                warn!(
                    trade_id = %event.trade_id,
                    account_id = %event.account_id,
                    reason = %reason,
                    "Settlement event rejected by governance policy"
                );
                return self
                    .commit_and_report(
                        envelope.metadata,
                        plan,
                        executed_stages,
                        ProcessingStage::Validate,
                        StageDecision::CommitAndReport,
                        "validate",
                        ProcessingOutcome::PolicyRejected {
                            reason: reason.clone(),
                        },
                        reason,
                    )
                    .await;
            }
        }

        let deduplicate_stage = Self::advance_stage(
            ProcessingStage::Validate,
            StageDecision::Continue,
            &mut executed_stages,
        );
        debug_assert_eq!(deduplicate_stage, Some(ProcessingStage::Deduplicate));
        self.tools.observer.observe(
            "deduplicate",
            &envelope.metadata,
            "Checking idempotency boundary for trade",
        );

        match self.engine.begin_settlement(&event.trade_id).await {
            TradeClaim::Duplicate => {
                warn!(
                    trade_id = %event.trade_id,
                    account_id = %event.account_id,
                    "Duplicate trade event detected. Skipping re-settlement."
                );
                return self
                    .commit_and_report(
                        envelope.metadata,
                        plan,
                        executed_stages,
                        ProcessingStage::Deduplicate,
                        StageDecision::CommitAndReport,
                        "deduplicate",
                        ProcessingOutcome::Duplicate,
                        "Duplicate trade skipped".to_string(),
                    )
                    .await;
            }
            TradeClaim::InFlight => {
                warn!(
                    trade_id = %event.trade_id,
                    account_id = %event.account_id,
                    "Trade is already in flight; leaving offset uncommitted for retry."
                );
                let report_stage = Self::advance_stage(
                    ProcessingStage::Deduplicate,
                    StageDecision::ReportOnly,
                    &mut executed_stages,
                );
                debug_assert_eq!(report_stage, Some(ProcessingStage::Report));
                let detail = "Trade is already in flight".to_string();
                self.tools
                    .observer
                    .observe("report", &envelope.metadata, &detail);
                return Ok(ProcessingReport {
                    metadata: envelope.metadata,
                    outcome: ProcessingOutcome::ProcessingFailed {
                        reason: detail.clone(),
                    },
                    detail,
                    committed: false,
                    executed_stages,
                    plan,
                });
            }
            TradeClaim::Acquired => {}
        }

        let settle_stage = Self::advance_stage(
            ProcessingStage::Deduplicate,
            StageDecision::Continue,
            &mut executed_stages,
        );
        debug_assert_eq!(settle_stage, Some(ProcessingStage::Settle));
        self.tools.observer.observe(
            "settle",
            &envelope.metadata,
            "Applying ledger settlement for claimed trade",
        );

        if let Err(err) = self.engine.apply_settlement(&event).await {
            self.engine.release_settlement(&event.trade_id).await;
            error!(
                error = ?err,
                trade_id = %event.trade_id,
                account_id = %event.account_id,
                "Settlement failed. Halting offset commit."
            );

            let report_stage = Self::advance_stage(
                ProcessingStage::Settle,
                StageDecision::ReportOnly,
                &mut executed_stages,
            );
            debug_assert_eq!(report_stage, Some(ProcessingStage::Report));
            self.tools.observer.observe(
                "report",
                &envelope.metadata,
                "Settlement failed before offset commit",
            );

            return Ok(ProcessingReport {
                metadata: envelope.metadata,
                outcome: ProcessingOutcome::ProcessingFailed {
                    reason: err.to_string(),
                },
                detail: err.to_string(),
                committed: false,
                executed_stages,
                plan,
            });
        }

        self.engine.complete_settlement(&event.trade_id).await;

        self.commit_and_report(
            envelope.metadata,
            plan,
            executed_stages,
            ProcessingStage::Settle,
            StageDecision::Continue,
            "settle",
            ProcessingOutcome::Applied,
            "Settlement applied".to_string(),
        )
        .await
    }

    async fn commit_and_report(
        &self,
        metadata: MessageMetadata,
        plan: ExecutionPlan,
        mut executed_stages: Vec<ProcessingStage>,
        current_stage: ProcessingStage,
        current_decision: StageDecision,
        observe_stage: &str,
        outcome: ProcessingOutcome,
        detail: String,
    ) -> Result<ProcessingReport> {
        self.tools
            .observer
            .observe(observe_stage, &metadata, &detail);

        let commit_stage =
            Self::advance_stage(current_stage, current_decision, &mut executed_stages);
        debug_assert_eq!(commit_stage, Some(ProcessingStage::Commit));

        let committed = match &outcome {
            ProcessingOutcome::InvalidPayload | ProcessingOutcome::EmptyPayload => {
                self.criteria.commit_on_invalid_payload
            }
            ProcessingOutcome::Duplicate => self.criteria.commit_on_duplicates,
            ProcessingOutcome::Applied => self.criteria.commit_on_success,
            ProcessingOutcome::PolicyRejected { .. } => true,
            ProcessingOutcome::ProcessingFailed { .. } => false,
        };

        if committed {
            self.tools.committer.commit(&metadata).await?;
        }

        let report_stage = Self::advance_stage(
            ProcessingStage::Commit,
            StageDecision::Continue,
            &mut executed_stages,
        );
        debug_assert_eq!(report_stage, Some(ProcessingStage::Report));
        self.tools
            .observer
            .observe("report", &metadata, "Generated terminal processing report");

        info!(
            topic = %metadata.topic,
            partition = metadata.partition,
            offset = metadata.offset,
            committed = committed,
            outcome = ?outcome,
            "Settlement message processed"
        );

        Ok(ProcessingReport {
            metadata,
            outcome,
            detail,
            committed,
            executed_stages,
            plan,
        })
    }

    fn advance_stage(
        current: ProcessingStage,
        decision: StageDecision,
        executed_stages: &mut Vec<ProcessingStage>,
    ) -> Option<ProcessingStage> {
        let next = ProcessingGraph::next_stage(current, decision);
        if let Some(stage) = next {
            executed_stages.push(stage);
        }
        next
    }
}

impl<P, D, L, Dec, Comm, Obs, Pol> EnvelopeProcessor
    for SettlementOrchestrator<P, D, L, Dec, Comm, Obs, Pol>
where
    P: Planner,
    D: DeduplicationStore,
    L: LedgerSettlementTool,
    Dec: PayloadDecoder,
    Comm: OffsetCommitTool,
    Obs: ObserverTool,
    Pol: GovernancePolicy,
{
    fn process_envelope<'a>(
        &'a self,
        envelope: MessageEnvelope,
    ) -> BoxFuture<'a, Result<ProcessingReport>> {
        Box::pin(async move { self.process_message(envelope).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        InMemoryDeduplicationStore, LoggingLedgerSettlementTool, SettlementEngine,
    };
    use crate::planner::{ProcessingStage, SettlementPlanner};
    use crate::policy::SettlementPolicy;
    use crate::tools::{
        JsonPayloadDecoder, MessageEnvelope, MessageMetadata, ObserverTool, OffsetCommitTool,
        ToolRegistry,
    };
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct RecordingCommitter {
        commits: Mutex<Vec<MessageMetadata>>,
    }

    impl OffsetCommitTool for RecordingCommitter {
        fn commit<'a>(&'a self, metadata: &'a MessageMetadata) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                self.commits.lock().await.push(metadata.clone());
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct NoopObserver;

    impl ObserverTool for NoopObserver {
        fn observe(&self, _stage: &str, _metadata: &MessageMetadata, _detail: &str) {}
    }

    fn metadata() -> MessageMetadata {
        MessageMetadata {
            topic: "trades.settlement.v1".to_string(),
            partition: 0,
            offset: 7,
            retry_count: 0,
        }
    }

    struct FailOnceLedger {
        fail_once: Mutex<bool>,
    }

    impl LedgerSettlementTool for FailOnceLedger {
        fn apply<'a>(
            &'a self,
            _event: &'a crate::engine::SettlementEvent,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                let mut fail_once = self.fail_once.lock().await;
                if *fail_once {
                    *fail_once = false;
                    anyhow::bail!("simulated ledger failure");
                }
                Ok(())
            })
        }
    }

    fn orchestrator() -> SettlementOrchestrator<
        SettlementPlanner,
        InMemoryDeduplicationStore,
        LoggingLedgerSettlementTool,
        JsonPayloadDecoder,
        RecordingCommitter,
        NoopObserver,
        SettlementPolicy,
    > {
        SettlementOrchestrator::new(
            SettlementPlanner,
            SettlementEngine::default(),
            ToolRegistry::new(
                JsonPayloadDecoder,
                RecordingCommitter::default(),
                NoopObserver,
            ),
            SettlementPolicy,
            4,
        )
    }

    #[tokio::test]
    async fn commits_after_successful_processing() {
        let orchestrator = orchestrator();
        let report = orchestrator
            .process_message(MessageEnvelope {
                metadata: metadata(),
                payload: Some(
                    br#"{"trade_id":"trade-123","account_id":"acct-42","asset_pair":"BTC/USD","amount_micros":150000,"execution_timestamp":1726437600}"#
                        .to_vec(),
                ),
            })
            .await
            .unwrap();

        assert_eq!(report.outcome, ProcessingOutcome::Applied);
        assert_eq!(report.detail, "Settlement applied");
        assert!(report.committed);
        assert_eq!(
            report.executed_stages,
            vec![
                ProcessingStage::Ingest,
                ProcessingStage::Validate,
                ProcessingStage::Deduplicate,
                ProcessingStage::Settle,
                ProcessingStage::Commit,
                ProcessingStage::Report,
            ]
        );
    }

    #[tokio::test]
    async fn commits_and_skips_invalid_payloads() {
        let orchestrator = orchestrator();
        let report = orchestrator
            .process_message(MessageEnvelope {
                metadata: metadata(),
                payload: Some(br#"{"trade_id":42}"#.to_vec()),
            })
            .await
            .unwrap();

        assert_eq!(report.outcome, ProcessingOutcome::InvalidPayload);
        assert!(report
            .detail
            .contains("Failed to decode settlement event payload"));
        assert!(report.committed);
        assert_eq!(
            report.executed_stages,
            vec![
                ProcessingStage::Ingest,
                ProcessingStage::Validate,
                ProcessingStage::Commit,
                ProcessingStage::Report,
            ]
        );
    }

    #[tokio::test]
    async fn commits_duplicate_messages_without_reapplying_settlement() {
        let orchestrator = orchestrator();
        let envelope = MessageEnvelope {
            metadata: metadata(),
            payload: Some(
                br#"{"trade_id":"trade-123","account_id":"acct-42","asset_pair":"BTC/USD","amount_micros":150000,"execution_timestamp":1726437600}"#
                    .to_vec(),
            ),
        };

        let first = orchestrator
            .process_message(envelope.clone())
            .await
            .unwrap();
        let second = orchestrator.process_message(envelope).await.unwrap();

        assert_eq!(first.outcome, ProcessingOutcome::Applied);
        assert_eq!(second.outcome, ProcessingOutcome::Duplicate);
        assert!(second.committed);
        assert_eq!(
            second.executed_stages,
            vec![
                ProcessingStage::Ingest,
                ProcessingStage::Validate,
                ProcessingStage::Deduplicate,
                ProcessingStage::Commit,
                ProcessingStage::Report,
            ]
        );
    }

    #[tokio::test]
    async fn commits_empty_payload_messages() {
        let orchestrator = orchestrator();
        let report = orchestrator
            .process_message(MessageEnvelope {
                metadata: metadata(),
                payload: None,
            })
            .await
            .unwrap();

        assert_eq!(report.outcome, ProcessingOutcome::EmptyPayload);
        assert_eq!(report.detail, "settlement message payload is empty");
        assert!(report.committed);
    }

    #[tokio::test]
    async fn failed_settlement_is_retryable_after_claim_release() {
        let orchestrator = SettlementOrchestrator::new(
            SettlementPlanner,
            SettlementEngine::with_components(
                InMemoryDeduplicationStore::default(),
                FailOnceLedger {
                    fail_once: Mutex::new(true),
                },
            ),
            ToolRegistry::new(
                JsonPayloadDecoder,
                RecordingCommitter::default(),
                NoopObserver,
            ),
            SettlementPolicy,
            4,
        );
        let envelope = MessageEnvelope {
            metadata: metadata(),
            payload: Some(
                br#"{"trade_id":"trade-123","account_id":"acct-42","asset_pair":"BTC/USD","amount_micros":150000,"execution_timestamp":1726437600}"#
                    .to_vec(),
            ),
        };

        let first = orchestrator
            .process_message(envelope.clone())
            .await
            .unwrap();
        let second = orchestrator.process_message(envelope).await.unwrap();

        assert_eq!(
            first.outcome,
            ProcessingOutcome::ProcessingFailed {
                reason: "simulated ledger failure".to_string(),
            }
        );
        assert!(!first.committed);
        assert_eq!(second.outcome, ProcessingOutcome::Applied);
        assert!(second.committed);
    }
}
