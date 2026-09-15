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

#[derive(Debug, Error)]
pub enum SettlementError {
    #[error("settlement message payload is empty")]
    EmptyPayload,
}

pub trait DeduplicationStore: Send + Sync {
    fn claim_trade<'a>(&'a self, trade_id: &'a str) -> BoxFuture<'a, bool>;
}

pub trait LedgerSettlementTool: Send + Sync {
    fn apply<'a>(&'a self, event: &'a SettlementEvent) -> BoxFuture<'a, Result<()>>;
}

#[derive(Debug, Default)]
pub struct InMemoryDeduplicationStore {
    processed_trades: Mutex<HashSet<String>>,
}

impl DeduplicationStore for InMemoryDeduplicationStore {
    fn claim_trade<'a>(&'a self, trade_id: &'a str) -> BoxFuture<'a, bool> {
        Box::pin(async move {
            let mut processed_trades = self.processed_trades.lock().await;
            processed_trades.insert(trade_id.to_owned())
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

    pub async fn claim_trade(&self, trade_id: &str) -> bool {
        self.deduplication_store.claim_trade(trade_id).await
    }

    pub async fn apply_settlement(&self, event: &SettlementEvent) -> Result<()> {
        self.ledger_tool.apply(event).await
    }

    pub async fn process_settlement(&self, event: SettlementEvent) -> Result<SettlementOutcome> {
        if !self.claim_trade(&event.trade_id).await {
            warn!(
                trade_id = %event.trade_id,
                "Duplicate trade event detected. Skipping re-settlement."
            );
            return Ok(SettlementOutcome::Duplicate);
        }

        self.apply_settlement(&event).await?;
        Ok(SettlementOutcome::Applied)
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
}
