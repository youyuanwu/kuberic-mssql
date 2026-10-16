use std::error::Error;
use std::fmt;

pub const MSSQL_FAILOVER_DELAY_SECONDS: u64 = 30;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlGroupError {
    message: String,
    termination_unconfirmed: bool,
}

impl MssqlGroupError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            termination_unconfirmed: false,
        }
    }

    pub(crate) fn termination_unconfirmed(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            termination_unconfirmed: true,
        }
    }

    pub fn agent_termination_unconfirmed(&self) -> bool {
        self.termination_unconfirmed
    }
}

impl fmt::Display for MssqlGroupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for MssqlGroupError {}
