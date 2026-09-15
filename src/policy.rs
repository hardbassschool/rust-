use serde::{Deserialize, Serialize};

use crate::engine::SettlementEvent;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessingCriteria {
    pub commit_on_invalid_payload: bool,
    pub commit_on_duplicates: bool,
    pub commit_on_success: bool,
}

impl Default for ProcessingCriteria {
    fn default() -> Self {
        Self {
            commit_on_invalid_payload: true,
            commit_on_duplicates: true,
            commit_on_success: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyAction {
    Continue,
    SkipAndCommit { reason: String },
}

pub trait GovernancePolicy: Send + Sync {
    fn evaluate(&self, event: &SettlementEvent) -> PolicyAction;
}

#[derive(Debug, Default)]
pub struct SettlementPolicy;

impl GovernancePolicy for SettlementPolicy {
    fn evaluate(&self, event: &SettlementEvent) -> PolicyAction {
        if event.trade_id.trim().is_empty() {
            return PolicyAction::SkipAndCommit {
                reason: "trade_id must not be empty".to_string(),
            };
        }

        if event.account_id.trim().is_empty() {
            return PolicyAction::SkipAndCommit {
                reason: "account_id must not be empty".to_string(),
            };
        }

        if event.asset_pair.trim().is_empty() {
            return PolicyAction::SkipAndCommit {
                reason: "asset_pair must not be empty".to_string(),
            };
        }

        PolicyAction::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_rejects_empty_trade_id() {
        let policy = SettlementPolicy;
        let action = policy.evaluate(&SettlementEvent {
            trade_id: String::new(),
            account_id: "acct-42".to_string(),
            asset_pair: "BTC/USD".to_string(),
            amount_micros: 1,
            execution_timestamp: 1,
        });

        assert_eq!(
            action,
            PolicyAction::SkipAndCommit {
                reason: "trade_id must not be empty".to_string(),
            }
        );
    }
}
