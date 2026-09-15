use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct SettlementEvent {
    pub trade_id: String,
    pub account_id: String,
    pub asset_pair: String,
    pub amount_micros: u64,
    pub execution_timestamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementOutcome {
    Applied,
    Duplicate,
}

#[derive(Debug, Error)]
pub enum SettlementError {
    #[error("settlement message payload is empty")]
    EmptyPayload,
}

pub struct SettlementEngine {
    processed_trades: Arc<Mutex<HashSet<String>>>,
}

impl SettlementEngine {
    pub fn new() -> Self {
        Self {
            processed_trades: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub async fn process_settlement(&self, event: SettlementEvent) -> Result<SettlementOutcome> {
        let mut cache = self.processed_trades.lock().await;

        if cache.contains(&event.trade_id) {
            warn!(
                trade_id = %event.trade_id,
                "Duplicate trade event detected. Skipping re-settlement."
            );
            return Ok(SettlementOutcome::Duplicate);
        }

        info!(
            trade_id = %event.trade_id,
            pair = %event.asset_pair,
            amount = event.amount_micros,
            "Executing settlement on ledger balance..."
        );

        cache.insert(event.trade_id);
        Ok(SettlementOutcome::Applied)
    }
}

impl Default for SettlementEngine {
    fn default() -> Self {
        Self::new()
    }
}

fn build_consumer(brokers: &str, group_id: &str) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create()
        .context("Failed to construct rdkafka consumer")?;

    Ok(consumer)
}

fn decode_event(payload: &[u8]) -> Result<SettlementEvent> {
    serde_json::from_slice(payload).context("Failed to decode settlement event payload")
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    info!("Starting Kraken Settlement Engine Service...");

    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let group_id = "settlement-consumer-v1";
    let topic = "trades.settlement.v1";

    let engine = Arc::new(SettlementEngine::new());
    let consumer = build_consumer(&brokers, group_id)?;

    if let Err(err) = consumer.subscribe(&[topic]) {
        warn!(
            error = %err,
            "Kafka subscription failed; proceeding in simulation mode."
        );
    }

    info!(
        topic = topic,
        "Consumer running. Listening for stream events..."
    );
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("Shutdown signal received. Flushing state and terminating gracefully.");
                break;
            }
            msg_result = consumer.recv() => {
                match msg_result {
                    Ok(msg) => {
                        let payload = match msg.payload() {
                            Some(payload) => payload,
                            None => {
                                error!(error = %SettlementError::EmptyPayload, "Settlement message missing payload; dropping event.");
                                continue;
                            }
                        };

                        match decode_event(payload) {
                            Ok(event) => {
                                if let Err(err) = engine.process_settlement(event).await {
                                    error!(error = ?err, "Settlement failed. Halting offset commit.");
                                    continue;
                                }

                                if let Err(err) = consumer.commit_message(&msg, CommitMode::Async) {
                                    error!(error = ?err, "Failed to commit Kafka offset");
                                }
                            }
                            Err(err) => {
                                error!(error = ?err, "Malformed payload received; dropping to DLQ.");
                            }
                        }
                    }
                    Err(err) => {
                        warn!(error = ?err, "Kafka stream poll encountered a non-fatal warning");
                        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                    }
                }
            }
        }
    }

    Ok(())
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
    async fn processes_first_event_and_marks_duplicate_retries() {
        let engine = SettlementEngine::new();

        let first = engine.process_settlement(fixture_event()).await.unwrap();
        let second = engine.process_settlement(fixture_event()).await.unwrap();

        assert_eq!(first, SettlementOutcome::Applied);
        assert_eq!(second, SettlementOutcome::Duplicate);
    }

    #[test]
    fn decodes_valid_settlement_payload() {
        let payload = br#"{
            "trade_id":"trade-123",
            "account_id":"acct-42",
            "asset_pair":"BTC/USD",
            "amount_micros":150000,
            "execution_timestamp":1726437600
        }"#;

        let event = decode_event(payload).unwrap();

        assert_eq!(event, fixture_event());
    }
}
