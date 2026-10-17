use std::sync::{Arc, Mutex};

use futures::future::{BoxFuture, FutureExt, Shared};
use tokio::sync::watch;

use super::KubericAdapterError;

pub(super) type OwnedResult<T> = Shared<BoxFuture<'static, Result<T, KubericAdapterError>>>;

#[derive(Clone)]
struct OwnedTask {
    result: watch::Receiver<Option<Result<(), KubericAdapterError>>>,
    completion: OwnedResult<()>,
}

#[derive(Default)]
struct State {
    sealed: bool,
    callbacks: usize,
    tasks: Vec<OwnedTask>,
}

pub(super) struct OwnedWork {
    state: Mutex<State>,
    changed: watch::Sender<u64>,
}

impl OwnedWork {
    pub(super) fn new() -> Arc<Self> {
        let (changed, _) = watch::channel(0);
        Arc::new(Self {
            state: Mutex::new(State::default()),
            changed,
        })
    }

    pub(super) fn enter(self: &Arc<Self>) -> Result<OwnedOperation, KubericAdapterError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.sealed {
            return Err(KubericAdapterError::Closed);
        }
        state.callbacks += 1;
        Ok(OwnedOperation(self.clone()))
    }

    pub(super) fn seal(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sealed = true;
        self.notify();
    }

    pub(super) fn spawn<T>(
        &self,
        spawn: impl FnOnce() -> tokio::task::JoinHandle<T>,
        failure: fn(&T) -> Option<KubericAdapterError>,
    ) -> Result<OwnedResult<T>, KubericAdapterError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.sealed {
            return Err(KubericAdapterError::Closed);
        }
        state
            .tasks
            .retain(|task| !matches!(task.result.borrow().as_ref(), Some(Ok(()))));
        let task = spawn();
        let completion = async move {
            task.await.map_err(|_| {
                KubericAdapterError::NativeConvergenceUnsafe("owned MSSQL worker terminated")
            })
        }
        .boxed()
        .shared();
        let observed = completion.clone();
        let (result, result_rx) = watch::channel(None);
        let owner = tokio::spawn(async move {
            let outcome = observed.await;
            let status = match &outcome {
                Ok(value) => failure(value).map_or(Ok(()), Err),
                Err(error) => Err(error.clone()),
            };
            drop(outcome);
            result.send_replace(Some(status.clone()));
            status
        });
        let drain = async move {
            owner.await.map_err(|_| {
                KubericAdapterError::NativeConvergenceUnsafe(
                    "owned MSSQL completion owner terminated",
                )
            })?
        }
        .boxed()
        .shared();
        state.tasks.push(OwnedTask {
            result: result_rx,
            completion: drain,
        });
        Ok(completion)
    }

    pub(super) async fn wait(&self) -> Result<(), KubericAdapterError> {
        let mut changed = self.changed.subscribe();
        let tasks = loop {
            let tasks = {
                let state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if !state.sealed {
                    return Err(KubericAdapterError::InvalidConfiguration(
                        "local containment requires Close or Abort",
                    ));
                }
                (state.callbacks == 0).then(|| state.tasks.clone())
            };
            if let Some(tasks) = tasks {
                break tasks;
            }
            changed.changed().await.map_err(|_| {
                KubericAdapterError::NativeConvergenceUnsafe("owned MSSQL drainage owner lost")
            })?;
        };
        let mut failure = None;
        for task in tasks {
            if let Err(error) = task.completion.await {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    fn notify(&self) {
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }
}

pub(super) struct OwnedOperation(Arc<OwnedWork>);

impl Drop for OwnedOperation {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .callbacks -= 1;
        self.0.notify();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::runtime_host::seed_state::SeedStateError;

    #[tokio::test]
    async fn cancelled_waiter_retains_the_worker_join() {
        let work = OwnedWork::new();
        let (release, held) = tokio::sync::oneshot::channel();
        let result = work
            .spawn(
                || {
                    tokio::spawn(async move {
                        held.await.unwrap();
                        7
                    })
                },
                |_| None,
            )
            .unwrap();
        work.seal();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), work.wait())
                .await
                .is_err()
        );
        release.send(()).unwrap();
        work.wait().await.unwrap();
        assert_eq!(result.await.unwrap(), 7);
        work.wait().await.unwrap();
    }

    #[tokio::test]
    async fn worker_failure_survives_caller_loss_and_repeated_drains() {
        let work = OwnedWork::new();
        let result = work
            .spawn(
                || tokio::task::spawn_blocking(|| Err::<(), _>(SeedStateError::Io)),
                |result| {
                    result
                        .as_ref()
                        .err()
                        .map(|error| KubericAdapterError::SeedState(*error))
                },
            )
            .unwrap();
        drop(result);
        work.seal();
        for _ in 0..2 {
            assert_eq!(
                work.wait().await,
                Err(KubericAdapterError::SeedState(SeedStateError::Io))
            );
        }
    }

    #[tokio::test]
    async fn worker_panic_survives_caller_loss_and_repeated_drains() {
        let work = OwnedWork::new();
        let result = work
            .spawn(
                || tokio::task::spawn_blocking(|| panic!("injected owned worker panic")),
                |_: &()| None,
            )
            .unwrap();
        drop(result);
        work.seal();
        for _ in 0..2 {
            assert_eq!(
                work.wait().await,
                Err(KubericAdapterError::NativeConvergenceUnsafe(
                    "owned MSSQL worker terminated"
                )),
            );
        }
    }

    #[tokio::test]
    async fn sealed_registry_never_spawns_late_work() {
        let work = OwnedWork::new();
        work.seal();
        let invoked = AtomicBool::new(false);
        assert!(work.enter().is_err());
        assert!(
            work.spawn(
                || {
                    invoked.store(true, Ordering::SeqCst);
                    tokio::spawn(async {})
                },
                |_| None,
            )
            .is_err()
        );
        assert!(!invoked.load(Ordering::SeqCst));
        work.wait().await.unwrap();
    }

    #[tokio::test]
    async fn drainage_waits_for_admitted_callback_ownership() {
        let work = OwnedWork::new();
        let callback = work.enter().unwrap();
        work.seal();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), work.wait())
                .await
                .is_err()
        );
        drop(callback);
        work.wait().await.unwrap();
    }
}
