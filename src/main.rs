mod config;
#[cfg(test)]
mod exporter_extra_tests;
mod logs;
mod metrics;
#[cfg(test)]
mod shutdown_tests;
mod tarpit;

use std::{process::ExitCode, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::watch, time::Instant};

fn main() -> ExitCode {
    let result = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .map_err(|_| "cannot initialize runtime")
        .and_then(|runtime| {
            let result = runtime.block_on(run());
            // Cleanup already used the shutdown budget; DNS must not extend it.
            runtime.shutdown_timeout(Duration::ZERO);
            result
        });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("tarssh-rs: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), &'static str> {
    let config = Arc::new(config::Config::parse(|key| {
        std::env::var_os(key).map(|v| v.into_string().unwrap_or_else(|_| "\u{fffd}".into()))
    })?);
    let metrics =
        metrics::Metrics::new(&config.deployment).map_err(|_| "cannot initialize metrics")?;
    let client = logs::client().map_err(|_| "cannot initialize HTTP client")?;
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| "cannot install SIGTERM handler")?;
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|_| "cannot install SIGINT handler")?;
    let listener = TcpListener::bind(config.listen)
        .await
        .map_err(|_| "cannot bind TARSSH_LISTEN_ADDR")?;
    let metrics_listener = TcpListener::bind(config.metrics)
        .await
        .map_err(|_| "cannot bind TARSSH_METRICS_ADDR")?;
    let (stop, shutdown) = watch::channel(None);
    let (logs, rx) = logs::Logs::channel(metrics.clone(), config.deployment.clone());
    logs.event("service_started", serde_json::json!({}));
    let mut exporter = tokio::spawn(logs::export(
        rx,
        metrics.clone(),
        config.deployment.clone(),
        config.alloy.clone(),
        client,
        shutdown.clone(),
    ));
    let mut connections = tokio::spawn(tarpit::serve(
        listener,
        config,
        metrics.clone(),
        logs.clone(),
        shutdown.clone(),
    ));
    let mut metrics_shutdown = shutdown;
    let mut http = tokio::spawn(async move {
        axum::serve(metrics_listener, metrics.router())
            .with_graceful_shutdown(async move {
                if metrics_shutdown.borrow().is_none() {
                    let _ = metrics_shutdown.changed().await;
                }
            })
            .await
    });
    let mut connections_finished = false;
    let mut http_finished = false;
    let mut exporter_finished = false;
    let signal = async {
        #[cfg(unix)]
        tokio::select! { _ = terminate.recv() => {}, _ = interrupt.recv() => {} }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    };
    // Runtime failures stay off stdout/stderr and trigger bounded cleanup.
    tokio::select! {
        _ = signal => {},
        _ = &mut connections => connections_finished = true,
        _ = &mut http => http_finished = true,
        _ = &mut exporter => exporter_finished = true,
    }
    let end = Instant::now() + Duration::from_secs(10);
    logs.event("service_stopping", serde_json::json!({}));
    let _ = stop.send(Some(end));
    if !connections_finished
        && tokio::time::timeout_at(end, &mut connections)
            .await
            .is_err()
    {
        connections.abort();
        let _ = connections.await;
    }
    drop(logs);
    if !http_finished && tokio::time::timeout_at(end, &mut http).await.is_err() {
        http.abort();
        let _ = http.await;
    }
    if !exporter_finished && tokio::time::timeout_at(end, &mut exporter).await.is_err() {
        exporter.abort();
        let _ = exporter.await;
    }
    Ok(())
}
