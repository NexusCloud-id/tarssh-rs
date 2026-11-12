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
            let original_hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(|_| {}));
            let result = runtime.block_on(run());
            let _ = std::panic::take_hook();
            std::panic::set_hook(original_hook);
            // Cleanup already used the shutdown budget; DNS must not extend it.
            runtime.shutdown_timeout(Duration::ZERO);
            result
        });
    match result {
        Ok(status) => status,
        Err(error) => {
            eprintln!("tarssh-rs: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<ExitCode, &'static str> {
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
    let exporter = tokio::spawn(logs::export(
        rx,
        metrics.clone(),
        config.deployment.clone(),
        config.alloy.clone(),
        client,
        shutdown.clone(),
    ));
    let connections = tokio::spawn(tarpit::serve(
        listener,
        config,
        metrics.clone(),
        logs.clone(),
        shutdown.clone(),
    ));
    let mut metrics_shutdown = shutdown;
    let http = tokio::spawn(async move {
        axum::serve(metrics_listener, metrics.router())
            .with_graceful_shutdown(async move {
                if metrics_shutdown.borrow().is_none() {
                    let _ = metrics_shutdown.changed().await;
                }
            })
            .await
    });
    let signal = async {
        #[cfg(unix)]
        tokio::select! { _ = terminate.recv() => {}, _ = interrupt.recv() => {} }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    };
    Ok(supervise(connections, http, exporter, stop, logs, signal).await)
}

async fn supervise(
    mut connections: tokio::task::JoinHandle<()>,
    mut http: tokio::task::JoinHandle<std::io::Result<()>>,
    mut exporter: tokio::task::JoinHandle<()>,
    stop: watch::Sender<Option<Instant>>,
    logs: logs::Logs,
    signal: impl std::future::Future<Output = ()>,
) -> ExitCode {
    enum CompletionCause {
        Clean,
        HttpInnerError,
        JoinError,
    }
    let mut connections_finished = false;
    let mut http_finished = false;
    let mut exporter_finished = false;
    // Runtime failures stay off stdout/stderr and trigger bounded cleanup.
    tokio::task::yield_now().await;
    let cause = tokio::select! {
        biased;
        result = &mut connections => {
            connections_finished = true;
            Some(match result {
                Ok(()) => CompletionCause::Clean,
                Err(_) => CompletionCause::JoinError,
            })
        },
        result = &mut http => {
            http_finished = true;
            Some(match result {
                Ok(Ok(())) => CompletionCause::Clean,
                Ok(Err(_)) => CompletionCause::HttpInnerError,
                Err(_) => CompletionCause::JoinError,
            })
        },
        result = &mut exporter => {
            exporter_finished = true;
            Some(match result {
                Ok(()) => CompletionCause::Clean,
                Err(_) => CompletionCause::JoinError,
            })
        },
        _ = signal => None,
    };
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
    if cause.is_some() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
