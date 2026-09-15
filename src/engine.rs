use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{info, warn};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct SettlementEvent {
    pub trade_id: String,
    pub account_id: String,
    pub asset_pair: String,
    pub amount_micros: u64,
    pub execution_timestamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SettlementOutcome {
    Applied,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeClaim {
    Acquired,
    Duplicate,
    InFlight,
}

#[derive(Debug, Error)]
pub enum SettlementError {
    #[error("settlement message payload is empty")]
    EmptyPayload,
}

pub trait DeduplicationStore: Send + Sync {
    fn begin_trade<'a>(&'a self, trade_id: &'a str) -> BoxFuture<'a, TradeClaim>;
    fn complete_trade<'a>(&'a self, trade_id: &'a str) -> BoxFuture<'a, ()>;
    fn release_trade<'a>(&'a self, trade_id: &'a str) -> BoxFuture<'a, ()>;
}

pub trait LedgerSettlementTool: Send + Sync {
    fn apply<'a>(&'a self, event: &'a SettlementEvent) -> BoxFuture<'a, Result<()>>;
}

#[derive(Debug, Default)]
pub struct InMemoryDeduplicationStore {
    state: Mutex<DeduplicationState>,
}

#[derive(Debug, Default)]
struct DeduplicationState {
    completed_trades: HashSet<String>,
    inflight_trades: HashSet<String>,
}

impl DeduplicationStore for InMemoryDeduplicationStore {
    fn begin_trade<'a>(&'a self, trade_id: &'a str) -> BoxFuture<'a, TradeClaim> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            if state.completed_trades.contains(trade_id) {
                TradeClaim::Duplicate
            } else if state.inflight_trades.contains(trade_id) {
                TradeClaim::InFlight
            } else {
                state.inflight_trades.insert(trade_id.to_owned());
                TradeClaim::Acquired
            }
        })
    }

    fn complete_trade<'a>(&'a self, trade_id: &'a str) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            state.inflight_trades.remove(trade_id);
            state.completed_trades.insert(trade_id.to_owned());
        })
    }

    fn release_trade<'a>(&'a self, trade_id: &'a str) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            state.inflight_trades.remove(trade_id);
        })
    }
}

#[derive(Debug, Default)]
pub struct LoggingLedgerSettlementTool;

impl LedgerSettlementTool for LoggingLedgerSettlementTool {
    fn apply<'a>(&'a self, event: &'a SettlementEvent) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            info!(
                trade_id = %event.trade_id,
                account_id = %event.account_id,
                pair = %event.asset_pair,
                amount = event.amount_micros,
                "Executing settlement on ledger balance..."
            );
            Ok(())
        })
    }
}

pub struct SettlementEngine<D = InMemoryDeduplicationStore, L = LoggingLedgerSettlementTool> {
    deduplication_store: Arc<D>,
    ledger_tool: Arc<L>,
}

impl<D, L> SettlementEngine<D, L>
where
    D: DeduplicationStore,
    L: LedgerSettlementTool,
{
    pub fn with_components(deduplication_store: D, ledger_tool: L) -> Self {
        Self {
            deduplication_store: Arc::new(deduplication_store),
            ledger_tool: Arc::new(ledger_tool),
        }
    }

    pub async fn begin_settlement(&self, trade_id: &str) -> TradeClaim {
        self.deduplication_store.begin_trade(trade_id).await
    }

    pub async fn complete_settlement(&self, trade_id: &str) {
        self.deduplication_store.complete_trade(trade_id).await
    }

    pub async fn release_settlement(&self, trade_id: &str) {
        self.deduplication_store.release_trade(trade_id).await
    }

    pub async fn apply_settlement(&self, event: &SettlementEvent) -> Result<()> {
        self.ledger_tool.apply(event).await
    }

    pub async fn process_settlement(&self, event: SettlementEvent) -> Result<SettlementOutcome> {
        match self.begin_settlement(&event.trade_id).await {
            TradeClaim::Duplicate | TradeClaim::InFlight => {
                warn!(
                    trade_id = %event.trade_id,
                    "Duplicate trade event detected. Skipping re-settlement."
                );
                Ok(SettlementOutcome::Duplicate)
            }
            TradeClaim::Acquired => {
                if let Err(err) = self.apply_settlement(&event).await {
                    self.release_settlement(&event.trade_id).await;
                    return Err(err);
                }

                self.complete_settlement(&event.trade_id).await;
                Ok(SettlementOutcome::Applied)
            }
        }
    }
}

impl Default for SettlementEngine<InMemoryDeduplicationStore, LoggingLedgerSettlementTool> {
    fn default() -> Self {
        Self::with_components(
            InMemoryDeduplicationStore::default(),
            LoggingLedgerSettlementTool,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_event() -> SettlementEvent {
        SettlementEvent {
            trade_id: "trade-123".to_string(),
            account_id: "acct-42".to_string(),
            asset_pair: "BTC/USD".to_string(),
            amount_micros: 150_000,
            execution_timestamp: 1_726_437_600,
        }
    }

    #[tokio::test]
    async fn process_settlement_marks_duplicate_retries() {
        let engine = SettlementEngine::default();

        let first = engine.process_settlement(fixture_event()).await.unwrap();
        let second = engine.process_settlement(fixture_event()).await.unwrap();

        assert_eq!(first, SettlementOutcome::Applied);
        assert_eq!(second, SettlementOutcome::Duplicate);
    }

    struct FailingLedger {
        fail_once: Mutex<bool>,
    }

    impl LedgerSettlementTool for FailingLedger {
        fn apply<'a>(&'a self, _event: &'a SettlementEvent) -> BoxFuture<'a, Result<()>> {
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

    #[tokio::test]
    async fn failed_settlement_releases_claim_for_retry() {
        let engine = SettlementEngine::with_components(
            InMemoryDeduplicationStore::default(),
            FailingLedger {
                fail_once: Mutex::new(true),
            },
        );

        let first = engine.process_settlement(fixture_event()).await;
        let second = engine.process_settlement(fixture_event()).await.unwrap();

        assert!(first.is_err());
        assert_eq!(second, SettlementOutcome::Applied);
    }
}
