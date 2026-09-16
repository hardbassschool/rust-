use anyhow::{Context, Result};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

use crate::engine::{BoxFuture, SettlementEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum ToolKind {
    DecodePayload,
    IdempotencyClaim,
    ApplyLedgerSettlement,
    CommitOffset,
    EmitObservation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub retry_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageEnvelope {
    pub metadata: MessageMetadata,
    pub payload: Option<Vec<u8>>,
}

pub trait PayloadDecoder: Send + Sync {
    fn decode(&self, payload: &[u8]) -> Result<SettlementEvent>;
}

pub trait OffsetCommitTool: Send + Sync {
    fn commit<'a>(&'a self, metadata: &'a MessageMetadata) -> BoxFuture<'a, Result<()>>;
}

pub trait ObserverTool: Send + Sync {
    fn observe(&self, stage: &str, metadata: &MessageMetadata, detail: &str);
}

#[derive(Debug, Default)]
pub struct JsonPayloadDecoder;

impl PayloadDecoder for JsonPayloadDecoder {
    fn decode(&self, payload: &[u8]) -> Result<SettlementEvent> {
        serde_json::from_slice(payload).context("Failed to decode settlement event payload")
    }
}

pub struct KafkaOffsetCommitter {
    consumer: Arc<StreamConsumer>,
}

impl KafkaOffsetCommitter {
    pub fn new(consumer: Arc<StreamConsumer>) -> Self {
        Self { consumer }
    }
}

impl OffsetCommitTool for KafkaOffsetCommitter {
    fn commit<'a>(&'a self, metadata: &'a MessageMetadata) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let next_offset = metadata
                .offset
                .checked_add(1)
                .context("Kafka offset overflow while preparing commit")?;

            let mut partitions = TopicPartitionList::new();
            partitions
                .add_partition_offset(
                    &metadata.topic,
                    metadata.partition,
                    Offset::Offset(next_offset),
                )
                .context("Failed to prepare Kafka offset commit")?;

            self.consumer
                .commit(&partitions, CommitMode::Async)
                .context("Failed to commit Kafka offset")?;

            Ok(())
        })
    }
}

#[derive(Debug, Default)]
pub struct TracingObserver;

impl ObserverTool for TracingObserver {
    fn observe(&self, stage: &str, metadata: &MessageMetadata, detail: &str) {
        info!(
            stage = stage,
            topic = %metadata.topic,
            partition = metadata.partition,
            offset = metadata.offset,
            retry_count = metadata.retry_count,
            detail = detail,
            "Settlement pipeline stage"
        );
    }
}

pub struct ToolRegistry<D, C, O> {
    pub decoder: Arc<D>,
    pub committer: Arc<C>,
    pub observer: Arc<O>,
}

impl<D, C, O> ToolRegistry<D, C, O> {
    pub fn new(decoder: D, committer: C, observer: O) -> Self {
        Self {
            decoder: Arc::new(decoder),
            committer: Arc::new(committer),
            observer: Arc::new(observer),
        }
    }
}
