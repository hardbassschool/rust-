use serde::{Deserialize, Serialize};

use crate::planner::{ExecutionPlan, Planner, ProcessingGoal};
use crate::tools::ToolKind;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleProfile {
    pub name: &'static str,
    pub responsibility: &'static str,
    pub capabilities: Vec<ToolKind>,
}

pub struct PlannerAgent<P> {
    planner: P,
    profile: RoleProfile,
}

impl<P> PlannerAgent<P>
where
    P: Planner,
{
    pub fn new(planner: P) -> Self {
        Self {
            planner,
            profile: RoleProfile {
                name: "planner",
                responsibility: "Transforms a goal into explicit executable stages.",
                capabilities: vec![ToolKind::EmitObservation],
            },
        }
    }

    pub fn profile(&self) -> &RoleProfile {
        &self.profile
    }

    pub fn plan(&self, goal: ProcessingGoal) -> ExecutionPlan {
        self.planner.build_plan(goal)
    }
}

#[derive(Debug)]
pub struct ExecutorAgent {
    profile: RoleProfile,
}

impl ExecutorAgent {
    pub fn new() -> Self {
        Self {
            profile: RoleProfile {
                name: "executor",
                responsibility:
                    "Runs the plan with decoding, deduplication, settlement, and commit tools.",
                capabilities: vec![
                    ToolKind::DecodePayload,
                    ToolKind::IdempotencyClaim,
                    ToolKind::ApplyLedgerSettlement,
                    ToolKind::CommitOffset,
                    ToolKind::EmitObservation,
                ],
            },
        }
    }

    pub fn profile(&self) -> &RoleProfile {
        &self.profile
    }
}

#[derive(Debug)]
pub struct CriticAgent {
    profile: RoleProfile,
}

impl CriticAgent {
    pub fn new() -> Self {
        Self {
            profile: RoleProfile {
                name: "critic",
                responsibility: "Checks commit policy and terminal outcome invariants.",
                capabilities: vec![ToolKind::EmitObservation],
            },
        }
    }

    pub fn profile(&self) -> &RoleProfile {
        &self.profile
    }

    pub fn requires_terminal_report(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct ReporterAgent {
    profile: RoleProfile,
}

impl ReporterAgent {
    pub fn new() -> Self {
        Self {
            profile: RoleProfile {
                name: "reporter",
                responsibility: "Summarizes the processing outcome into structured report fields.",
                capabilities: vec![ToolKind::EmitObservation],
            },
        }
    }

    pub fn profile(&self) -> &RoleProfile {
        &self.profile
    }

    pub fn report_fields(&self) -> &'static [&'static str] {
        &["topic", "partition", "offset", "outcome", "committed"]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{ProcessingGoal, ProcessingStage, SettlementPlanner};

    #[test]
    fn planner_agent_exposes_expected_plan() {
        let agent = PlannerAgent::new(SettlementPlanner);
        let plan = agent.plan(ProcessingGoal::HandleSettlementEvent);

        assert_eq!(agent.profile().name, "planner");
        assert_eq!(plan.steps.first().unwrap().stage, ProcessingStage::Ingest);
    }

    #[test]
    fn executor_agent_has_commit_capability() {
        let agent = ExecutorAgent::new();

        assert!(agent
            .profile()
            .capabilities
            .contains(&ToolKind::CommitOffset));
    }
}
