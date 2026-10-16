use std::error::Error;
use std::fmt;
use std::time::Duration;

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

#[doc(hidden)]
pub async fn exercise_bounded_host_shutdown_fence() -> MssqlGroupError {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut task = tokio::task::spawn_blocking(move || {
        entered_tx.send(()).ok();
        release_rx.recv().ok();
    });
    entered_rx
        .await
        .expect("bounded shutdown task must enter its uninterruptible section");

    task.abort();
    let error = match tokio::time::timeout(Duration::ZERO, &mut task).await {
        Err(_) => MssqlGroupError::termination_unconfirmed(
            "host task termination could not be established within the cleanup budget",
        ),
        Ok(_) => MssqlGroupError::new(
            "host task terminated before the exhausted cleanup budget was observed",
        ),
    };

    release_tx.send(()).ok();
    let _ = tokio::time::timeout(Duration::from_secs(1), &mut task).await;
    error
}
