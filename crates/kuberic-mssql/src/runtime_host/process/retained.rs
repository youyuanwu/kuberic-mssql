use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;
use tokio::sync::{oneshot, watch};
use tokio_util::sync::CancellationToken;

use super::*;

type Outcome = Result<(), RuntimeProcessError>;
type Reporter = Arc<dyn Fn(&RuntimeProcessError) + Send + Sync>;

#[derive(Default)]
struct Results {
    bounded: Option<Outcome>,
    completed: Option<Outcome>,
    host: Option<Outcome>,
    departed: bool,
    consumed: bool,
    cancelled: bool,
    reported: bool,
    #[cfg(feature = "kuberic-testing")]
    diagnostic: Option<RuntimeProcessError>,
}

struct Completion {
    results: Mutex<Results>,
    changed: watch::Sender<()>,
    stop: CancellationToken,
    report: Reporter,
}

impl Completion {
    fn report(&self, error: RuntimeProcessError) {
        (self.report)(&error);
        #[cfg(feature = "kuberic-testing")]
        {
            self.results.lock().unwrap().diagnostic = Some(error);
            self.changed.send_replace(());
        }
    }

    fn new(report: Reporter) -> Arc<Self> {
        Arc::new(Self {
            results: Mutex::new(Results::default()),
            changed: watch::channel(()).0,
            stop: CancellationToken::new(),
            report,
        })
    }

    fn with_cancellation(results: &Results, result: Outcome) -> Outcome {
        if results.cancelled {
            combine(
                Err(RuntimeProcessError::new(
                    "runtime process waiter cancelled; containment unconfirmed",
                )),
                result,
                "retained cleanup",
            )
        } else {
            result
        }
    }

    fn unobserved(results: &mut Results) -> Option<RuntimeProcessError> {
        if results.departed && !results.consumed && !results.reported {
            let result = Self::with_cancellation(results, results.completed.clone()?);
            if let Err(error) = result {
                results.reported = true;
                return Some(error);
            }
        }
        None
    }

    fn publish(&self, result: Outcome, final_result: bool) {
        let diagnostic = {
            let mut results = self.results.lock().unwrap();
            if final_result {
                results.completed = Some(result);
            } else {
                results.bounded = Some(result);
            }
            Self::unobserved(&mut results)
        };
        self.changed.send_replace(());
        if let Some(error) = diagnostic {
            self.report(error);
        }
    }

    fn bounded_cleanup(&self, host: &Outcome, cleanup: Outcome) {
        let host = self
            .results
            .lock()
            .unwrap()
            .host
            .clone()
            .unwrap_or_else(|| host.clone());
        self.publish(combine(host, cleanup, "application shutdown"), false);
    }

    fn depart(&self, cancelled: bool) {
        let diagnostic = {
            let mut results = self.results.lock().unwrap();
            results.departed = true;
            results.cancelled |= cancelled;
            Self::unobserved(&mut results)
        };
        if cancelled {
            self.stop.cancel();
        }
        if let Some(error) = diagnostic {
            self.report(error);
        }
    }

    #[cfg(test)]
    fn observed(&self) -> Option<Outcome> {
        let results = self.results.lock().unwrap();
        results
            .completed
            .clone()
            .map(|result| Self::with_cancellation(&results, result))
    }
}

pub(super) struct Waiter<A: StatefulServiceReplica> {
    application: Arc<A>,
    completion: Arc<Completion>,
    armed: bool,
    #[cfg(feature = "kuberic-testing")]
    owner: Option<futures::future::Shared<futures::future::BoxFuture<'static, ()>>>,
}

impl<A: StatefulServiceReplica> Waiter<A> {
    #[cfg(feature = "kuberic-testing")]
    pub(super) fn testing_completion(&self) -> TestingRuntimeCompletion {
        TestingRuntimeCompletion {
            completion: self.completion.clone(),
            owner: self
                .owner
                .clone()
                .expect("owner was spawned before observation"),
        }
    }

    pub(super) async fn wait(mut self) -> Outcome {
        let mut changed = self.completion.changed.subscribe();
        loop {
            let result = {
                let mut results = self.completion.results.lock().unwrap();
                if let Some(result) = results.completed.clone() {
                    results.consumed = true;
                    Some(result)
                } else {
                    results.bounded.clone()
                }
            };
            if let Some(result) = result {
                self.armed = false;
                self.completion.depart(false);
                return result;
            }
            changed
                .changed()
                .await
                .expect("retained completion owns sender");
        }
    }
}

/// Observe the production owner's final joins without consuming its waiter result
/// or suppressing the production fallback diagnostic.
#[cfg(feature = "kuberic-testing")]
#[doc(hidden)]
#[derive(Clone)]
pub struct TestingRuntimeCompletion {
    completion: Arc<Completion>,
    owner: futures::future::Shared<futures::future::BoxFuture<'static, ()>>,
}

#[cfg(feature = "kuberic-testing")]
impl TestingRuntimeCompletion {
    pub fn is_finished(&self) -> bool {
        self.completion.results.lock().unwrap().completed.is_some()
    }

    pub async fn wait(&self) -> Outcome {
        self.owner.clone().await;
        let results = self.completion.results.lock().unwrap();
        Completion::with_cancellation(
            &results,
            results
                .completed
                .clone()
                .expect("joined owner published result"),
        )
    }

    pub async fn diagnostic(&self) -> RuntimeProcessError {
        let mut changed = self.completion.changed.subscribe();
        loop {
            if let Some(error) = self.completion.results.lock().unwrap().diagnostic.clone() {
                return error;
            }
            changed.changed().await.expect("completion owns sender");
        }
    }
}

impl<A: StatefulServiceReplica> Drop for Waiter<A> {
    fn drop(&mut self) {
        if self.armed {
            self.application.abort();
            self.completion.depart(true);
        }
    }
}

pub(super) fn start<A, S, SF, F>(
    application: Arc<A>,
    concrete: Option<Arc<RuntimeHostApplication>>,
    startup: S,
    shutdown: F,
    deadline: Duration,
) -> Waiter<A>
where
    A: StatefulServiceReplica + 'static,
    S: FnOnce(watch::Receiver<bool>) -> SF + Send + 'static,
    SF: Future<Output = kuberic_runtime::host::Result<Option<RunningReplica>>> + Send + 'static,
    F: Future<Output = Outcome> + Send + 'static,
{
    // Each applicable stage gets one configured budget, never reset by another
    // stop request: startup-stop, host-stop, post-Abort host-stop, Close, drain.
    // Thus the conservative composed acknowledgement bound is five budgets
    // (normally fewer, since Abort skips Close), excluding executor scheduling.
    // Expiry publishes failure; the same owner still drives real completion.
    // None of this outlives destruction of the async runtime or the OS process.
    start_with_reporter(
        application,
        concrete,
        startup,
        shutdown,
        deadline,
        Arc::new(|error| eprintln!("kuberic-mssql-runtime: retained cleanup: {error}")),
    )
}

fn start_with_reporter<A, S, SF, F>(
    application: Arc<A>,
    concrete: Option<Arc<RuntimeHostApplication>>,
    startup: S,
    shutdown: F,
    deadline: Duration,
    reporter: Reporter,
) -> Waiter<A>
where
    A: StatefulServiceReplica + 'static,
    S: FnOnce(watch::Receiver<bool>) -> SF + Send + 'static,
    SF: Future<Output = kuberic_runtime::host::Result<Option<RunningReplica>>> + Send + 'static,
    F: Future<Output = Outcome> + Send + 'static,
{
    start_with_cleanup(
        application,
        startup,
        shutdown,
        deadline,
        reporter,
        move |application, completion, host, pending| async move {
            let aborted = concrete
                .as_ref()
                .map(|app| &app.aborted)
                .unwrap_or(&completion.stop);
            let drain = concrete.as_ref().map(|app| async {
                app.wait_for_containment()
                    .await
                    .map_err(RuntimeProcessError::application)
            });
            finish_application(
                application.as_ref(),
                aborted,
                drain,
                &completion,
                &host,
                pending,
                deadline,
            )
            .await
        },
    )
}

fn start_with_cleanup<A, S, SF, F, C, CF>(
    application: Arc<A>,
    startup: S,
    shutdown: F,
    deadline: Duration,
    reporter: Reporter,
    cleanup: C,
) -> Waiter<A>
where
    A: StatefulServiceReplica + 'static,
    S: FnOnce(watch::Receiver<bool>) -> SF + Send + 'static,
    SF: Future<Output = kuberic_runtime::host::Result<Option<RunningReplica>>> + Send + 'static,
    F: Future<Output = Outcome> + Send + 'static,
    C: FnOnce(Arc<A>, Arc<Completion>, Outcome, bool) -> CF + Send + 'static,
    CF: Future<Output = Outcome> + Send,
{
    let completion = Completion::new(reporter);
    // Arm before spawning even the owner: no independent host work precedes it.
    let waiter = Waiter {
        application: application.clone(),
        completion: completion.clone(),
        armed: true,
        #[cfg(feature = "kuberic-testing")]
        owner: None,
    };
    let owner = tokio::spawn(async move {
        let (ack, acknowledged) = oneshot::channel();
        let host = async {
            let mut ack = Some(ack);
            let result = std::panic::AssertUnwindSafe(drive_host(
                application.as_ref(),
                startup,
                shutdown,
                &completion.stop,
                deadline,
                &mut ack,
            ))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| {
                application.abort();
                Err(RuntimeProcessError::new("runtime host lifecycle panicked"))
            });
            completion.results.lock().unwrap().host = Some(result.clone());
            acknowledge(&mut ack, result.clone(), false);
            result
        };
        let cleanup = async {
            let (host, pending) = acknowledged.await.expect("host always acknowledges");
            std::panic::AssertUnwindSafe(cleanup(
                application.clone(),
                completion.clone(),
                host,
                pending,
            ))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| {
                application.abort();
                Err(RuntimeProcessError::new(
                    "runtime application cleanup panicked; containment unconfirmed",
                ))
            })
        };
        let (host, cleanup) = tokio::join!(host, cleanup);
        completion.publish(combine(host, cleanup, "application shutdown"), true);
    });
    #[cfg(feature = "kuberic-testing")]
    {
        let mut waiter = waiter;
        waiter.owner = Some(
            async move { owner.await.expect("retained process owner joined") }
                .boxed()
                .shared(),
        );
        waiter
    }
    #[cfg(not(feature = "kuberic-testing"))]
    {
        drop(owner);
        waiter
    }
}

type Acknowledgement = Option<oneshot::Sender<(Outcome, bool)>>;

fn acknowledge(ack: &mut Acknowledgement, result: Outcome, pending: bool) {
    if let Some(ack) = ack.take() {
        // Acknowledgement is internal; loss of the public waiter cannot drop it.
        ack.send((result, pending))
            .expect("cleanup owns acknowledgement receiver");
    }
}

async fn stop_requested(
    shutdown: impl Future<Output = Outcome>,
    stop: &CancellationToken,
) -> Outcome {
    tokio::select! {
        biased;
        result = shutdown => result,
        _ = stop.cancelled() => Ok(()), // Cancellation is composed exactly once at publication.
    }
}

async fn drive_startup<T>(
    startup: impl Future<Output = Result<Option<T>, RuntimeProcessError>>,
    shutdown: impl Future<Output = Outcome>,
    signal: watch::Sender<bool>,
    application: &impl StatefulServiceReplica,
    deadline: Duration,
    ack: &mut Acknowledgement,
) -> (Result<Option<T>, RuntimeProcessError>, Option<Outcome>) {
    tokio::pin!(startup);
    let trigger = tokio::select! {
        result = &mut startup => return (result, None),
        result = shutdown => result,
    };
    signal.send_replace(true);
    match tokio::time::timeout(deadline, &mut startup).await {
        Ok(result) => (result, Some(trigger)),
        Err(_) => {
            application.abort();
            let trigger = combine(
                trigger,
                Err(RuntimeProcessError::new(
                    "runtime startup cancellation exceeded its deadline; containment unconfirmed",
                )),
                "startup cancellation",
            );
            acknowledge(ack, trigger.clone(), true);
            // Never abort the root of cooperative upstream startup. It still owns
            // nested control/prepared work, and may return a late RunningReplica.
            (startup.await, Some(trigger))
        }
    }
}

async fn drive_host<A, S, SF>(
    application: &A,
    startup: S,
    shutdown: impl Future<Output = Outcome>,
    stop: &CancellationToken,
    deadline: Duration,
    ack: &mut Acknowledgement,
) -> Outcome
where
    A: StatefulServiceReplica,
    S: FnOnce(watch::Receiver<bool>) -> SF,
    SF: Future<Output = kuberic_runtime::host::Result<Option<RunningReplica>>>,
{
    let shutdown = stop_requested(shutdown, stop);
    tokio::pin!(shutdown);
    let (signal, receiver) = watch::channel(false);
    let startup = async { startup(receiver).await.map_err(RuntimeProcessError::host) };
    let (started, trigger) =
        drive_startup(startup, &mut shutdown, signal, application, deadline, ack).await;
    let mut replica = match started {
        Ok(Some(replica)) => replica,
        result => {
            return combine(
                trigger.unwrap_or(Ok(())),
                result.map(|_| ()),
                "runtime startup",
            );
        }
    };
    let (completion, trigger) = match trigger {
        Some(trigger) => (None, trigger),
        None => tokio::select! {
            result = replica.wait() => (Some(result), Ok(())),
            result = &mut shutdown => (None, result),
        },
    };
    replica.shutdown();
    let completion = match completion {
        Some(result) => result.map_err(RuntimeProcessError::host),
        None => {
            let wait = async { replica.wait().await.map_err(RuntimeProcessError::host) };
            wait_for_replica(wait, application, deadline, &trigger, ack).await
        }
    };
    combine(trigger, completion, "runtime completion")
}

async fn wait_for_replica(
    wait: impl Future<Output = Outcome>,
    application: &impl StatefulServiceReplica,
    deadline: Duration,
    trigger: &Outcome,
    ack: &mut Acknowledgement,
) -> Outcome {
    tokio::pin!(wait);
    match tokio::time::timeout(deadline, &mut wait).await {
        Ok(result) => result,
        Err(_) => {
            application.abort();
            let timeout = Err(RuntimeProcessError::new(
                "runtime shutdown acknowledgement timed out; containment unconfirmed",
            ));
            match tokio::time::timeout(deadline, &mut wait).await {
                Ok(result) => combine(timeout, result, "runtime completion after abort"),
                Err(_) => {
                    let timeout = combine(
                        timeout,
                        Err(RuntimeProcessError::new(
                            "runtime completion after abort timed out; containment unconfirmed",
                        )),
                        "runtime completion after timeout",
                    );
                    acknowledge(
                        ack,
                        combine(trigger.clone(), timeout.clone(), "runtime completion"),
                        true,
                    );
                    combine(timeout, wait.await, "late runtime completion")
                }
            }
        }
    }
}

async fn finish_application(
    application: &impl StatefulServiceReplica,
    aborted: &CancellationToken,
    drain: Option<impl Future<Output = Outcome>>,
    completion: &Completion,
    host: &Outcome,
    host_pending: bool,
    deadline: Duration,
) -> Outcome {
    let close = if aborted.is_cancelled() {
        Ok(())
    } else {
        tokio::select! {
            biased;
            _ = aborted.cancelled() => Err(RuntimeProcessError::new(
                "runtime application Close interrupted by Abort",
            )),
            result = tokio::time::timeout(deadline, application.close()) => match result {
                Ok(result) => result.map_err(RuntimeProcessError::application),
                Err(_) => Err(RuntimeProcessError::new("runtime application shutdown timed out")),
            },
        }
    };
    if close.is_err() {
        application.abort();
    }
    let Some(drain) = drain else {
        // Generic test applications have no MSSQL-local containment contract.
        if host_pending {
            completion.bounded_cleanup(host, close.clone());
        }
        return close;
    };
    tokio::pin!(drain);
    let result = match tokio::time::timeout(deadline, &mut drain).await {
        Ok(result) => combine(
            close,
            result.map_err(RuntimeProcessError::application),
            "local drainage",
        ),
        Err(_) => {
            let timeout = combine(
                close,
                Err(RuntimeProcessError::new(
                    "runtime local drainage timed out; containment unconfirmed",
                )),
                "local drainage",
            );
            completion.bounded_cleanup(host, timeout.clone());
            return combine(
                timeout,
                drain.await.map_err(RuntimeProcessError::application),
                "late local drainage",
            );
        }
    };
    if host_pending {
        completion.bounded_cleanup(host, result.clone());
    }
    result
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct HeldApplication {
        aborted: CancellationToken,
        close_entered: CancellationToken,
        close_release: CancellationToken,
        drain_entered: CancellationToken,
        drain_release: CancellationToken,
        closes: AtomicUsize,
        fail_close: bool,
        fail_drain: bool,
    }

    impl HeldApplication {
        fn new(fail_close: bool, fail_drain: bool) -> Arc<Self> {
            Arc::new(Self {
                aborted: CancellationToken::new(),
                close_entered: CancellationToken::new(),
                close_release: CancellationToken::new(),
                drain_entered: CancellationToken::new(),
                drain_release: CancellationToken::new(),
                closes: AtomicUsize::new(0),
                fail_close,
                fail_drain,
            })
        }

        async fn drain(&self) -> Outcome {
            self.drain_entered.cancel();
            self.drain_release.cancelled().await;
            if self.fail_drain {
                failure("late local drain failure")
            } else {
                Ok(())
            }
        }
    }

    #[async_trait]
    impl StatefulServiceReplica for HeldApplication {
        async fn open(self: Arc<Self>, _: OpenContext) -> KubericResult<Arc<dyn Replicator>> {
            panic!("ownership test must not open");
        }

        async fn change_role(
            &self,
            _: kuberic_runtime::protocol::types::ReplicaRole,
        ) -> KubericResult<RoleChange> {
            panic!("ownership test must not change role");
        }

        async fn close(&self) -> KubericResult<()> {
            assert!(
                !self.aborted.is_cancelled(),
                "no graceful callback after Abort"
            );
            self.closes.fetch_add(1, Ordering::SeqCst);
            self.close_entered.cancel();
            self.close_release.cancelled().await;
            if self.fail_close {
                Err(KubericRuntimeError::Application(
                    "original Close failure".into(),
                ))
            } else {
                Ok(())
            }
        }

        fn abort(&self) {
            self.aborted.cancel();
        }
    }

    fn failure(message: &str) -> Outcome {
        Err(RuntimeProcessError::new(message))
    }

    fn diagnostics() -> (Reporter, Arc<Mutex<Vec<String>>>) {
        let messages = Arc::new(Mutex::new(Vec::new()));
        let captured = messages.clone();
        (
            Arc::new(move |error| captured.lock().unwrap().push(error.to_string())),
            messages,
        )
    }

    fn launch<S, SF, F>(
        application: Arc<HeldApplication>,
        startup: S,
        shutdown: F,
        deadline: Duration,
        reporter: Reporter,
    ) -> Waiter<HeldApplication>
    where
        S: FnOnce(watch::Receiver<bool>) -> SF + Send + 'static,
        SF: Future<Output = kuberic_runtime::host::Result<Option<RunningReplica>>> + Send + 'static,
        F: Future<Output = Outcome> + Send + 'static,
    {
        start_with_cleanup(
            application,
            startup,
            shutdown,
            deadline,
            reporter,
            move |application, completion, host, host_pending| async move {
                finish_application(
                    application.as_ref(),
                    &application.aborted,
                    Some(application.drain()),
                    &completion,
                    &host,
                    host_pending,
                    deadline,
                )
                .await
            },
        )
    }

    async fn completed(completion: &Completion) -> Outcome {
        let mut changes = completion.changed.subscribe();
        loop {
            if let Some(result) = completion.observed() {
                return result;
            }
            changes.changed().await.unwrap();
        }
    }

    fn contains(result: Outcome, expected: &[&str]) {
        let message = result.unwrap_err().to_string();
        for part in expected {
            assert!(message.contains(part), "{part:?} missing from {message}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_close_waiter_retains_application_join_and_late_failure() {
        let app = HeldApplication::new(false, true);
        let weak = Arc::downgrade(&app);
        let release = app.drain_release.clone();
        let (report, diagnostics) = diagnostics();
        let waiter = launch(
            app.clone(),
            |_| async {
                Err(kuberic_runtime::host::HostError::CommandRejected(
                    "original runtime failure".into(),
                ))
            },
            pending(),
            Duration::from_secs(10),
            report,
        );
        let completion = waiter.completion.clone();
        let task = tokio::spawn(waiter.wait());
        app.close_entered.cancelled().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        app.drain_entered.cancelled().await;
        app.abort();
        app.abort();
        assert_eq!(app.closes.load(Ordering::SeqCst), 1);
        drop(app);
        assert!(
            weak.upgrade().is_some(),
            "owner retains actual application while drainage is held"
        );
        assert!(completion.observed().is_none());
        assert!(diagnostics.lock().unwrap().is_empty());
        release.cancel();
        contains(
            completed(&completion).await,
            &[
                "waiter cancelled",
                "original runtime failure",
                "Close interrupted",
                "late local drain failure",
            ],
        );
        assert!(weak.upgrade().is_none(), "actual join releases application");
        let messages = diagnostics.lock().unwrap();
        assert_eq!(messages.len(), 1);
        assert!(messages[0].contains("late local drain failure"));
        assert!(messages[0].contains("original runtime failure"));
    }

    #[tokio::test(start_paused = true)]
    async fn per_stage_deadlines_return_unconfirmed_then_retain_late_failure() {
        let budget = Duration::from_secs(10);
        let app = HeldApplication::new(false, true);
        let (report, diagnostics) = diagnostics();
        let waiter = launch(
            app.clone(),
            |mut stop| async move {
                stop.wait_for(|stopped| *stopped).await.unwrap();
                Ok(None)
            },
            async { failure("original trigger failure") },
            budget,
            report,
        );
        let completion = waiter.completion.clone();
        let task = tokio::spawn(waiter.wait());
        app.close_entered.cancelled().await;
        tokio::time::advance(budget).await;
        app.drain_entered.cancelled().await;
        assert!(
            !task.is_finished(),
            "Close and local drain have distinct budgets"
        );
        // Repeated Abort must neither start Close again nor reset the drain budget.
        tokio::time::advance(budget / 2).await;
        app.abort();
        app.abort();
        tokio::time::advance(budget / 2).await;
        contains(
            task.await.unwrap(),
            &[
                "original trigger failure",
                "application shutdown timed out",
                "local drainage timed out",
                "unconfirmed",
            ],
        );
        assert!(completion.observed().is_none());
        assert!(diagnostics.lock().unwrap().is_empty());
        app.drain_release.cancel();
        contains(
            completed(&completion).await,
            &[
                "original trigger failure",
                "application shutdown timed out",
                "local drainage timed out",
                "late local drain failure",
            ],
        );
        assert_eq!(app.closes.load(Ordering::SeqCst), 1);
        let messages = diagnostics.lock().unwrap();
        assert_eq!(
            messages.len(),
            1,
            "a result observer must not suppress stderr fallback"
        );
        assert!(messages[0].contains("late local drain failure"));
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_local_drain_waiter_keeps_the_same_join_and_result() {
        let app = HeldApplication::new(false, true);
        app.close_release.cancel();
        let (report, diagnostics) = diagnostics();
        let waiter = launch(
            app.clone(),
            |_| async {
                Err(kuberic_runtime::host::HostError::CommandRejected(
                    "original runtime failure".into(),
                ))
            },
            pending(),
            Duration::from_secs(10),
            report,
        );
        let completion = waiter.completion.clone();
        let task = tokio::spawn(waiter.wait());
        app.drain_entered.cancelled().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        app.abort();
        assert!(completion.observed().is_none());
        app.drain_release.cancel();
        contains(
            completed(&completion).await,
            &[
                "waiter cancelled",
                "original runtime failure",
                "late local drain failure",
            ],
        );
        assert_eq!(app.closes.load(Ordering::SeqCst), 1);
        assert_eq!(diagnostics.lock().unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_before_readiness_retains_startup_after_application_drain() {
        let app = HeldApplication::new(false, false);
        let started = CancellationToken::new();
        let stopped = CancellationToken::new();
        let release = CancellationToken::new();
        let start_gate = started.clone();
        let stop_gate = stopped.clone();
        let release_gate = release.clone();
        let (report, diagnostics) = diagnostics();
        let waiter = launch(
            app.clone(),
            move |mut stop| async move {
                let child = tokio::spawn(async move {
                    start_gate.cancel();
                    stop.wait_for(|stopped| *stopped).await.unwrap();
                    stop_gate.cancel();
                    release_gate.cancelled().await;
                    Err(kuberic_runtime::host::HostError::CommandRejected(
                        "late cancelled startup failure".into(),
                    ))
                });
                child.await.unwrap()
            },
            pending(),
            Duration::from_secs(10),
            report,
        );
        let completion = waiter.completion.clone();
        let task = tokio::spawn(waiter.wait());
        started.cancelled().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        stopped.cancelled().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        app.drain_entered.cancelled().await;
        app.drain_release.cancel();
        tokio::task::yield_now().await;
        assert!(
            completion.observed().is_none(),
            "local drain cannot stand in for startup completion"
        );
        assert_eq!(app.closes.load(Ordering::SeqCst), 0);
        release.cancel();
        contains(
            completed(&completion).await,
            &[
                "waiter cancelled",
                "startup cancellation exceeded",
                "late cancelled startup failure",
            ],
        );
        assert_eq!(diagnostics.lock().unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn published_but_unconsumed_failure_is_reported_on_waiter_loss() {
        let app = HeldApplication::new(true, true);
        app.close_release.cancel();
        app.drain_release.cancel();
        let (report, diagnostics) = diagnostics();
        let waiter = launch(
            app.clone(),
            |_| async { Ok(None) },
            pending(),
            Duration::from_secs(10),
            report,
        );
        let completion = waiter.completion.clone();
        contains(
            completed(&completion).await,
            &["original Close failure", "late local drain failure"],
        );
        assert!(
            diagnostics.lock().unwrap().is_empty(),
            "publication is not consumption"
        );
        drop(waiter);
        completion.depart(true);
        let messages = diagnostics.lock().unwrap();
        assert_eq!(messages.len(), 1);
        assert!(messages[0].contains("original Close failure"));
        assert!(messages[0].contains("late local drain failure"));
    }

    #[tokio::test(start_paused = true)]
    async fn consumed_final_error_needs_no_fallback() {
        let app = HeldApplication::new(true, true);
        app.close_release.cancel();
        app.drain_release.cancel();
        let (report, diagnostics) = diagnostics();
        let waiter = launch(
            app,
            |_| async { Ok(None) },
            pending(),
            Duration::from_secs(10),
            report,
        );
        contains(
            waiter.wait().await,
            &["original Close failure", "late local drain failure"],
        );
        assert!(diagnostics.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn host_initiated_abort_skips_close_and_joins_directly() {
        let app = HeldApplication::new(false, false);
        let host_app = app.clone();
        let (report, diagnostics) = diagnostics();
        let waiter = launch(
            app.clone(),
            move |_| async move {
                host_app.abort();
                Ok(None)
            },
            pending(),
            Duration::from_secs(10),
            report,
        );
        let completion = waiter.completion.clone();
        app.drain_entered.cancelled().await;
        assert_eq!(app.closes.load(Ordering::SeqCst), 0);
        assert!(completion.observed().is_none());
        app.abort();
        app.abort();
        app.drain_release.cancel();
        waiter.wait().await.unwrap();
        assert!(diagnostics.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn startup_timeout_keeps_cooperative_child_and_late_error_owned() {
        let app = HeldApplication::new(false, false);
        let budget = Duration::from_secs(10);
        let started = CancellationToken::new();
        let release = CancellationToken::new();
        let start_gate = started.clone();
        let release_gate = release.clone();
        let (report, diagnostics) = diagnostics();
        let waiter = launch(
            app.clone(),
            move |mut stop| async move {
                // The startup root owns an independently spawned child, just as
                // start_with_shutdown owns its control/prepared work.
                let child = tokio::spawn(async move {
                    start_gate.cancel();
                    stop.wait_for(|stopped| *stopped).await.unwrap();
                    release_gate.cancelled().await;
                    Err(kuberic_runtime::host::HostError::CommandRejected(
                        "late startup child failure".into(),
                    ))
                });
                child.await.unwrap()
            },
            {
                let started = started.clone();
                async move {
                    started.cancelled().await;
                    failure("startup trigger failure")
                }
            },
            budget,
            report,
        );
        let completion = waiter.completion.clone();
        let task = tokio::spawn(waiter.wait());
        started.cancelled().await;
        tokio::task::yield_now().await;
        tokio::time::advance(budget).await;
        app.drain_entered.cancelled().await;
        assert!(app.aborted.is_cancelled());
        assert_eq!(app.closes.load(Ordering::SeqCst), 0);
        app.drain_release.cancel();
        contains(
            task.await.unwrap(),
            &[
                "startup trigger failure",
                "startup cancellation exceeded",
                "unconfirmed",
            ],
        );
        assert!(
            completion.observed().is_none(),
            "startup root and child still owned"
        );
        release.cancel();
        contains(
            completed(&completion).await,
            &[
                "startup trigger failure",
                "startup cancellation exceeded",
                "late startup child failure",
            ],
        );
        assert_eq!(diagnostics.lock().unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn stop_readiness_race_retains_late_completion_handle() {
        let app = HeldApplication::new(false, false);
        let (signal, mut stop) = watch::channel(false);
        let release = CancellationToken::new();
        let child_release = release.clone();
        let startup = async move {
            let child = tokio::spawn(async move {
                child_release.cancelled().await;
                failure("late ready replica failure")
            });
            stop.wait_for(|stopped| *stopped).await.unwrap();
            Ok(Some(child))
        };
        let (ack, _acknowledged) = oneshot::channel();
        let mut ack = Some(ack);
        let (ready, trigger) = drive_startup(
            startup,
            async { failure("ready race trigger") },
            signal,
            app.as_ref(),
            Duration::from_secs(10),
            &mut ack,
        )
        .await;
        let handle = ready.unwrap().expect("stop raced with readiness");
        let trigger = trigger.unwrap();
        assert!(!handle.is_finished());
        release.cancel();
        let result = wait_for_replica(
            async { handle.await.unwrap() },
            app.as_ref(),
            Duration::from_secs(10),
            &trigger,
            &mut ack,
        )
        .await;
        contains(
            combine(trigger, result, "runtime completion"),
            &["ready race trigger", "late ready replica failure"],
        );
    }

    #[tokio::test(start_paused = true)]
    async fn both_host_ack_budgets_are_bounded_without_discarding_completion() {
        let app = HeldApplication::new(false, false);
        let release = CancellationToken::new();
        let child_release = release.clone();
        let (ack, acknowledged) = oneshot::channel();
        let host_app = app.clone();
        let task = tokio::spawn(async move {
            wait_for_replica(
                async {
                    child_release.cancelled().await;
                    failure("late runtime failure")
                },
                host_app.as_ref(),
                Duration::from_secs(10),
                &failure("runtime trigger failure"),
                &mut Some(ack),
            )
            .await
        });
        let (bounded, pending) = acknowledged.await.unwrap();
        assert!(pending);
        contains(
            bounded,
            &[
                "runtime trigger failure",
                "shutdown acknowledgement timed out",
                "completion after abort timed out",
            ],
        );
        assert!(app.aborted.is_cancelled());
        assert!(!task.is_finished());
        release.cancel();
        contains(
            task.await.unwrap(),
            &[
                "shutdown acknowledgement timed out",
                "completion after abort timed out",
                "late runtime failure",
            ],
        );
    }
}
