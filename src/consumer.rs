use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::orchestrator::EnvelopeProcessor;
use crate::tools::{MessageEnvelope, MessageMetadata};

pub fn build_consumer(brokers: &str, group_id: &str) -> Result<Arc<StreamConsumer>> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create()
        .context("Failed to construct rdkafka consumer")?;

    Ok(Arc::new(consumer))
}

pub fn subscribe_to_topic(consumer: &StreamConsumer, topic: &str) -> Result<()> {
    consumer
        .subscribe(&[topic])
        .context("Failed to subscribe settlement consumer to Kafka topic")
}

pub fn to_message_envelope(message: &BorrowedMessage<'_>) -> MessageEnvelope {
    MessageEnvelope {
        metadata: MessageMetadata {
            topic: message.topic().to_string(),
            partition: message.partition(),
            offset: message.offset(),
            retry_count: 0,
        },
        payload: message.payload().map(|payload| payload.to_vec()),
    }
}

pub async fn run_consumer_loop(
    consumer: Arc<StreamConsumer>,
    processor: Arc<dyn EnvelopeProcessor>,
) -> Result<()> {
    info!("Consumer running. Listening for stream events...");
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("Shutdown signal received. Flushing state and terminating gracefully.");
                break;
            }
            message = consumer.recv() => {
                match message {
                    Ok(message) => {
                        let envelope = to_message_envelope(&message);
                        if let Err(err) = processor.process_envelope(envelope).await {
                            error!(error = ?err, "Settlement processing failed before commit");
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
