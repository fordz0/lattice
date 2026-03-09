use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuleKind {
    PublisherKey,
    RecordKey,
    ContentHash,
    SiteName,
    PostId,
    CommentId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuleAction {
    Hide,
    RejectIngest,
    PurgeLocal,
    RefuseRepublish,
    Quarantine,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModerationRule {
    pub id: String,
    pub kind: RuleKind,
    pub value: String,
    pub action: RuleAction,
    pub created_at: u64,
    pub note: Option<String>,
}

pub struct ModerationEngine {
    rules: Vec<ModerationRule>,
}

impl ModerationEngine {
    pub fn load(rules: Vec<ModerationRule>) -> Self {
        Self { rules }
    }

    pub fn check_publisher(&self, pubkey_b64: &str) -> Option<&RuleAction> {
        self.match_rule(RuleKind::PublisherKey, pubkey_b64)
            .map(|rule| &rule.action)
    }

    pub fn check_key(&self, key: &str) -> Option<&RuleAction> {
        self.match_rule(RuleKind::RecordKey, key)
            .map(|rule| &rule.action)
    }

    pub fn check_hash(&self, hash_hex: &str) -> Option<&RuleAction> {
        self.match_rule(RuleKind::ContentHash, hash_hex)
            .map(|rule| &rule.action)
    }

    pub fn check_post(&self, post_id: &str) -> Option<&RuleAction> {
        self.match_rule(RuleKind::PostId, post_id)
            .map(|rule| &rule.action)
    }

    pub fn check_comment(&self, comment_id: &str) -> Option<&RuleAction> {
        self.match_rule(RuleKind::CommentId, comment_id)
            .map(|rule| &rule.action)
    }

    pub fn check_many(&self, checks: &[(RuleKind, &str)]) -> Option<&RuleAction> {
        for (kind, value) in checks {
            if let Some(rule) = self.match_rule(kind.clone(), value) {
                return Some(&rule.action);
            }
        }
        None
    }

    pub fn match_rule(&self, kind: RuleKind, value: &str) -> Option<&ModerationRule> {
        self.rules
            .iter()
            .find(|rule| rule.kind == kind && rule.value == value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_engine() -> ModerationEngine {
        ModerationEngine::load(vec![
            ModerationRule {
                id: "1".to_string(),
                kind: RuleKind::PublisherKey,
                value: "pubkey".to_string(),
                action: RuleAction::RejectIngest,
                created_at: 1,
                note: None,
            },
            ModerationRule {
                id: "2".to_string(),
                kind: RuleKind::RecordKey,
                value: "app:fray:feed:lattice".to_string(),
                action: RuleAction::Hide,
                created_at: 2,
                note: Some("hide feed".to_string()),
            },
            ModerationRule {
                id: "3".to_string(),
                kind: RuleKind::PostId,
                value: "post-1".to_string(),
                action: RuleAction::Quarantine,
                created_at: 3,
                note: None,
            },
        ])
    }

    #[test]
    fn checks_publisher_and_key() {
        let engine = sample_engine();
        assert_eq!(
            engine.check_publisher("pubkey"),
            Some(&RuleAction::RejectIngest)
        );
        assert_eq!(
            engine.check_key("app:fray:feed:lattice"),
            Some(&RuleAction::Hide)
        );
        assert_eq!(engine.check_key("app:fray:feed:other"), None);
    }

    #[test]
    fn checks_many_in_order() {
        let engine = sample_engine();
        let checks = vec![
            (RuleKind::CommentId, "comment-1"),
            (RuleKind::PostId, "post-1"),
            (RuleKind::PublisherKey, "pubkey"),
        ];
        assert_eq!(engine.check_many(&checks), Some(&RuleAction::Quarantine));
    }
}
