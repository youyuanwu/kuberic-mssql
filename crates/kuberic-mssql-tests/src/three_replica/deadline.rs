use std::future::Future;
use std::time::Duration;

use tokio::time::{Instant, timeout_at};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BoundedOperationError {
    Deadline,
    OperationTimeout,
}

pub(crate) async fn complete_before<T, F>(
    parent_deadline: Instant,
    operation_timeout: Duration,
    operation: F,
) -> Result<T, BoundedOperationError>
where
    F: Future<Output = T>,
{
    let now = Instant::now();
    if now >= parent_deadline {
        return Err(BoundedOperationError::Deadline);
    }
    let remaining = parent_deadline.saturating_duration_since(now);
    let limit = operation_timeout.min(remaining);
    let operation_deadline = now + limit;
    let parent_limited = remaining <= operation_timeout;
    let result = timeout_at(operation_deadline, operation)
        .await
        .map_err(|_| {
            if parent_limited {
                BoundedOperationError::Deadline
            } else {
                BoundedOperationError::OperationTimeout
            }
        })?;
    if Instant::now() >= operation_deadline {
        return Err(if parent_limited {
            BoundedOperationError::Deadline
        } else {
            BoundedOperationError::OperationTimeout
        });
    }
    Ok(result)
}

pub(crate) fn earlier(left: Instant, right: Instant) -> Instant {
    left.min(right)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test(start_paused = true)]
    async fn successful_result_ready_at_parent_deadline_is_rejected() {
        let parent = Instant::now() + Duration::from_millis(25);
        let result = complete_before(parent, Duration::from_secs(1), async {
            sleep(Duration::from_millis(25)).await;
            7
        })
        .await;
        assert_eq!(result, Err(BoundedOperationError::Deadline));
    }

    #[tokio::test(start_paused = true)]
    async fn operation_timeout_remains_distinct_before_parent_deadline() {
        let parent = Instant::now() + Duration::from_secs(1);
        let result = complete_before(parent, Duration::from_millis(25), async {
            sleep(Duration::from_millis(26)).await;
            7
        })
        .await;
        assert_eq!(result, Err(BoundedOperationError::OperationTimeout));
    }
}
