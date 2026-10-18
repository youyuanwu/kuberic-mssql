use super::*;

pub(super) fn consume_startup_result(
    result: Result<kuberic_runtime::host::Result<Option<RunningReplica>>, tokio::task::JoinError>,
) -> Result<RunningReplica, MssqlGroupError> {
    result
        .map_err(|error| {
            MssqlGroupError::termination_unconfirmed(format!("public startup task failed: {error}"))
        })?
        .map_err(|error| {
            MssqlGroupError::termination_unconfirmed(format!(
                "public startup failed without termination confirmation: {error}"
            ))
        })?
        .ok_or_else(|| MssqlGroupError::new("public host startup was cancelled"))
}

pub(super) async fn await_started_hosts(
    pods: &mut [PublicMssqlPod; 3],
    attempts: &mut [HostAttempt],
    budget: Duration,
) -> Result<(), MssqlGroupError> {
    for (index, attempt) in attempts.iter_mut().enumerate() {
        if pods[index].running.is_some() {
            continue;
        }
        let task = attempt.task.as_mut().ok_or_else(|| {
            MssqlGroupError::termination_unconfirmed("public host task ownership is missing")
        })?;
        let joined = timeout(budget, "public Kuberic readiness", task).await?;
        attempt.task.take();
        pods[index].running = Some(consume_startup_result(joined)?);
    }
    Ok(())
}

pub(super) fn startup_failure(
    primary: MssqlGroupError,
    shutdown: Result<(), MssqlGroupError>,
) -> MssqlGroupError {
    match shutdown {
        Ok(()) => primary,
        Err(shutdown) => MssqlGroupError::termination_unconfirmed(format!(
            "{primary}; public Kuberic startup cleanup failed: {shutdown}"
        )),
    }
}

pub(super) struct StartupHosts {
    pub(super) pods: Option<[PublicMssqlPod; 3]>,
    pub(super) attempts: Vec<HostAttempt>,
    pub(super) drained: Option<tokio::sync::oneshot::Sender<()>>,
    runtime: tokio::runtime::Handle,
}

impl StartupHosts {
    pub(super) fn new(pods: [PublicMssqlPod; 3]) -> Self {
        Self {
            pods: Some(pods),
            attempts: Vec::with_capacity(3),
            drained: None,
            runtime: tokio::runtime::Handle::current(),
        }
    }

    pub(super) fn take_pods(&mut self) -> [PublicMssqlPod; 3] {
        self.pods
            .take()
            .expect("startup retains all three pods until transfer")
    }
}

impl Drop for StartupHosts {
    fn drop(&mut self) {
        let Some(mut pods) = self.pods.take() else {
            return;
        };
        for attempt in &self.attempts {
            attempt.shutdown.send_replace(true);
        }
        for pod in &pods {
            if let Some(running) = &pod.running {
                running.shutdown();
            }
        }
        let attempts = std::mem::take(&mut self.attempts);
        let drained = self.drained.take();
        if attempts.iter().all(|attempt| attempt.task.is_none())
            && pods.iter().all(|pod| pod.running.is_none())
        {
            if let Some(drained) = drained {
                let _ = drained.send(());
            }
            return;
        }
        // Retain the owners after caller cancellation; this is not disposal confirmation.
        self.runtime.spawn(async move {
            let mut confirmed = true;
            for (index, attempt) in attempts.into_iter().enumerate() {
                if let Some(task) = attempt.task {
                    match task.await {
                        Ok(Ok(Some(running))) => pods[index].running = Some(running),
                        Ok(Ok(None)) => {}
                        Ok(Err(error)) => {
                            confirmed = false;
                            eprintln!("retained public startup failed: {error}");
                        }
                        Err(error) => {
                            confirmed = false;
                            eprintln!("retained public startup task failed: {error}");
                        }
                    }
                }
            }
            for pod in &mut pods {
                if let Some(mut running) = pod.running.take() {
                    running.shutdown();
                    if let Err(error) = running.wait().await {
                        confirmed = false;
                        eprintln!("retained public startup shutdown failed: {error}");
                    }
                }
            }
            if confirmed {
                if let Some(drained) = drained {
                    let _ = drained.send(());
                }
            }
        });
    }
}

pub(super) async fn cancel_attempts(
    attempts: &mut [HostAttempt],
    pods: &mut [PublicMssqlPod; 3],
    cleanup: &CleanupCoordinator<impl CleanupClock>,
) -> Result<(), MssqlGroupError> {
    for attempt in attempts.iter() {
        attempt.shutdown.send_replace(true);
    }
    for pod in pods.iter() {
        if let Some(running) = &pod.running {
            running.shutdown();
        }
    }
    let mut errors = Vec::new();
    for (index, attempt) in attempts.iter_mut().enumerate() {
        if let Some(task) = attempt.task.as_mut() {
            match tokio::time::timeout(cleanup.remaining().min(Duration::from_secs(10)), task).await
            {
                Ok(Ok(Ok(Some(running)))) => {
                    attempt.task.take();
                    running.shutdown();
                    pods[index].running = Some(running);
                }
                Ok(Ok(Ok(None)))
                | Ok(Ok(Err(HostError::Runtime(KubericRuntimeError::OperationCancelled)))) => {
                    attempt.task.take();
                }
                Ok(Ok(Err(error))) => {
                    attempt.task.take();
                    errors.push(error.to_string());
                }
                Ok(Err(error)) => {
                    attempt.task.take();
                    errors.push(error.to_string());
                }
                Err(_) => {
                    pods[index].application.abort();
                    errors.push(format!(
                        "member {} startup cancellation timed out",
                        index + 1
                    ));
                }
            }
        }
        let pod = &mut pods[index];
        if pod.running.is_some() {
            match await_public_host_shutdown(pod.ordinal, &mut pod.running, cleanup, || {
                pod.application.abort()
            })
            .await
            {
                PublicHostWait::Confirmed(Ok(())) => {}
                PublicHostWait::Confirmed(Err(error)) | PublicHostWait::Unconfirmed(error) => {
                    errors.push(error)
                }
            }
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(MssqlGroupError::termination_unconfirmed(format!(
            "public startup cleanup failed: {errors:?}"
        )))
    }
}
