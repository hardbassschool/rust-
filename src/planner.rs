use serde::{Deserialize, Serialize};

use crate::tools::ToolKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessingGoal {
    HandleSettlementEvent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessingStage {
    Ingest,
    Validate,
    Deduplicate,
    Settle,
    Commit,
    Report,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageDecision {
    Continue,
    CommitAndReport,
    ReportOnly,
    Halt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanStep {
    pub id: String,
    pub description: String,
    pub stage: ProcessingStage,
    pub required_tools: Vec<ToolKind>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub goal: ProcessingGoal,
    pub steps: Vec<PlanStep>,
}

pub trait Planner: Send + Sync {
    fn build_plan(&self, goal: ProcessingGoal) -> ExecutionPlan;
}

#[derive(Debug, Default)]
pub struct SettlementPlanner;

impl Planner for SettlementPlanner {
    fn build_plan(&self, goal: ProcessingGoal) -> ExecutionPlan {
        ExecutionPlan {
            goal,
            steps: vec![
                PlanStep {
                    id: "ingest".to_string(),
                    description: "Capture the raw event and metadata from Kafka.".to_string(),
                    stage: ProcessingStage::Ingest,
                    required_tools: vec![ToolKind::EmitObservation],
                },
                PlanStep {
                    id: "validate".to_string(),
                    description: "Decode and validate payload structure before processing."
                        .to_string(),
                    stage: ProcessingStage::Validate,
                    required_tools: vec![ToolKind::DecodePayload, ToolKind::EmitObservation],
                },
                PlanStep {
                    id: "deduplicate".to_string(),
                    description: "Claim the trade id through the idempotency boundary.".to_string(),
                    stage: ProcessingStage::Deduplicate,
                    required_tools: vec![ToolKind::IdempotencyClaim, ToolKind::EmitObservation],
                },
                PlanStep {
                    id: "settle".to_string(),
                    description: "Apply the settlement to the ledger only once.".to_string(),
                    stage: ProcessingStage::Settle,
                    required_tools: vec![
                        ToolKind::ApplyLedgerSettlement,
                        ToolKind::EmitObservation,
                    ],
                },
                PlanStep {
                    id: "commit".to_string(),
                    description: "Commit the Kafka offset after a terminal processing outcome."
                        .to_string(),
                    stage: ProcessingStage::Commit,
                    required_tools: vec![ToolKind::CommitOffset, ToolKind::EmitObservation],
                },
                PlanStep {
                    id: "report".to_string(),
                    description: "Emit a structured processing report for observability."
                        .to_string(),
                    stage: ProcessingStage::Report,
                    required_tools: vec![ToolKind::EmitObservation],
                },
            ],
        }
    }
}

pub struct ProcessingGraph;

impl ProcessingGraph {
    pub fn next_stage(stage: ProcessingStage, decision: StageDecision) -> Option<ProcessingStage> {
        match (stage, decision) {
            (ProcessingStage::Ingest, StageDecision::Continue) => Some(ProcessingStage::Validate),
            (ProcessingStage::Validate, StageDecision::Continue) => {
                Some(ProcessingStage::Deduplicate)
            }
            (ProcessingStage::Validate, StageDecision::CommitAndReport) => {
                Some(ProcessingStage::Commit)
            }
            (ProcessingStage::Deduplicate, StageDecision::Continue) => {
                Some(ProcessingStage::Settle)
            }
            (ProcessingStage::Deduplicate, StageDecision::CommitAndReport) => {
                Some(ProcessingStage::Commit)
            }
            (ProcessingStage::Settle, StageDecision::Continue) => Some(ProcessingStage::Commit),
            (ProcessingStage::Commit, StageDecision::Continue)
            | (ProcessingStage::Commit, StageDecision::ReportOnly) => Some(ProcessingStage::Report),
            (_, StageDecision::Halt) | (ProcessingStage::Report, _) => None,
            (_, StageDecision::ReportOnly) => Some(ProcessingStage::Report),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planner_builds_expected_stage_sequence() {
        let planner = SettlementPlanner;
        let plan = planner.build_plan(ProcessingGoal::HandleSettlementEvent);

        let stages: Vec<_> = plan.steps.iter().map(|step| step.stage).collect();

        assert_eq!(
            stages,
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

    #[test]
    fn graph_routes_invalid_payload_to_commit_then_report() {
        let commit =
            ProcessingGraph::next_stage(ProcessingStage::Validate, StageDecision::CommitAndReport);
        let report = ProcessingGraph::next_stage(ProcessingStage::Commit, StageDecision::Continue);

        assert_eq!(commit, Some(ProcessingStage::Commit));
        assert_eq!(report, Some(ProcessingStage::Report));
    }
}
