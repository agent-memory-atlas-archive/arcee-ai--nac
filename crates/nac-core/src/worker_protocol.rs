//! Private, versioned completion protocol over worker stdout and host stdin.
//! This carries one result for the admitted dispatch, never database commands.
use anyhow::Result;
use serde::{Deserialize, Serialize};

pub(crate) const COMPLETION_PREFIX: &str = "__NAC_COMPLETION_V1__";
pub(crate) const ACK_PREFIX: &str = "__NAC_COMMIT_ACK_V1__";
pub(crate) const MAX_COMPLETION_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_CONTROL_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Completion {
    pub session_id: String,
    pub thread_name: String,
    pub dispatch_id: String,
    pub content: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommitAck {
    pub session_id: String,
    pub thread_name: String,
    pub dispatch_id: String,
    pub episode_id: i64,
}

impl Completion {
    pub fn encode(&self) -> Result<String> {
        let line = format!("{COMPLETION_PREFIX}{}\n", serde_json::to_string(self)?);
        anyhow::ensure!(
            line.len() <= MAX_COMPLETION_BYTES,
            "worker completion exceeds protocol limit"
        );
        Ok(line)
    }

    pub fn validates_ack(&self, line: &str) -> bool {
        line.strip_prefix(ACK_PREFIX)
            .and_then(|payload| serde_json::from_str::<CommitAck>(payload).ok())
            .is_some_and(|ack| {
                ack.session_id == self.session_id
                    && ack.thread_name == self.thread_name
                    && ack.dispatch_id == self.dispatch_id
                    && ack.episode_id > 0
            })
    }
}
