use anyhow::Result;
use settlement_engine::consumer::{build_consumer, run_consumer_loop, subscribe_to_topic};
use settlement_engine::engine::{
    InMemoryDeduplicationStore, LoggingLedgerSettlementTool, SettlementEngine,
};
use settlement_engine::orchestrator::{EnvelopeProcessor, SettlementOrchestrator};
use settlement_engine::planner::SettlementPlanner;
use settlement_engine::policy::SettlementPolicy;
use settlement_engine::roles::{CriticAgent, ExecutorAgent, PlannerAgent, ReporterAgent};
use settlement_engine::tools::{
    JsonPayloadDecoder, KafkaOffsetCommitter, ToolRegistry, TracingObserver,
};
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    info!("Starting Kraken Settlement Engine Service...");

    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let group_id = "settlement-consumer-v1";
    let topic = "trades.settlement.v1";

    let consumer = build_consumer(&brokers, group_id)?;
    subscribe_to_topic(&consumer, topic)?;

    let _planner_role = PlannerAgent::new(SettlementPlanner);
    let _executor_role = ExecutorAgent::new();
    let _critic_role = CriticAgent::new();
    let _reporter_role = ReporterAgent::new();

    let orchestrator: Arc<dyn EnvelopeProcessor> = Arc::new(SettlementOrchestrator::new(
        SettlementPlanner,
        SettlementEngine::with_components(
            InMemoryDeduplicationStore::default(),
            LoggingLedgerSettlementTool,
        ),
        ToolRegistry::new(
            JsonPayloadDecoder,
            KafkaOffsetCommitter::new(consumer.clone()),
            TracingObserver,
        ),
        SettlementPolicy,
        32,
    ));

    run_consumer_loop(consumer, orchestrator).await
}
