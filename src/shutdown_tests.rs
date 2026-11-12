use crate::{
    logs::{self, Logs},
    metrics::Metrics,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{io::AsyncReadExt, net::TcpListener, sync::watch};

// Exercise real task completion, panic/JoinError and the HTTP inner result.
#[tokio::test(start_paused = true)]
async fn premature_subsystem_completion_fails_after_bounded_cleanup() {
    let mut failures = Vec::new();
    for subsystem in 0..3 {
        for panic in [false, true] {
            let (stop, mut shutdown) = watch::channel(None);
            let (events, _rx) = Logs::channel(Metrics::new("edge-1").unwrap(), "edge-1".into());
            let connections = tokio::spawn(async move {
                if subsystem == 0 {
                    assert!(!panic, "listener panic");
                } else {
                    std::future::pending::<()>().await;
                }
            });
            let http = tokio::spawn(async move {
                if subsystem == 1 {
                    assert!(!panic, "HTTP panic");
                    Ok(())
                } else {
                    std::future::pending().await
                }
            });
            let exporter = tokio::spawn(async move {
                if subsystem == 2 {
                    assert!(!panic, "exporter panic");
                } else {
                    std::future::pending::<()>().await;
                }
            });
            let started = tokio::time::Instant::now();
            let status = crate::supervise(
                connections,
                http,
                exporter,
                stop,
                events,
                std::future::pending(),
            )
            .await;
            if status != std::process::ExitCode::FAILURE {
                failures.push((subsystem, panic));
            }
            assert!(shutdown.changed().await.is_ok());
            assert_eq!(*shutdown.borrow(), Some(started + Duration::from_secs(10)));
            assert_eq!(
                tokio::time::Instant::now() - started,
                Duration::from_secs(10)
            );
        }
    }
    assert!(
        failures.is_empty(),
        "unexpected successful exits: {failures:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn premature_http_inner_error_fails() {
    let (stop, _shutdown) = watch::channel(None);
    let (events, _rx) = Logs::channel(Metrics::new("edge-1").unwrap(), "edge-1".into());
    let status = crate::supervise(
        tokio::spawn(std::future::pending()),
        tokio::spawn(async { Err(std::io::Error::other("HTTP failed")) }),
        tokio::spawn(std::future::pending()),
        stop,
        events,
        std::future::pending(),
    )
    .await;
    assert_eq!(status, std::process::ExitCode::FAILURE);
}

#[tokio::test(start_paused = true)]
async fn subsystem_completion_wins_over_ready_signal() {
    let (stop, _shutdown) = watch::channel(None);
    let (events, _rx) = Logs::channel(Metrics::new("edge-1").unwrap(), "edge-1".into());
    let status = crate::supervise(
        tokio::spawn(async {}),
        tokio::spawn(std::future::pending()),
        tokio::spawn(std::future::pending()),
        stop,
        events,
        async {},
    )
    .await;
    assert_eq!(status, std::process::ExitCode::FAILURE);
}

#[tokio::test(start_paused = true)]
async fn requested_signal_closes_sessions_before_exporter_and_succeeds() {
    let (stop, mut shutdown) = watch::channel(None);
    let (events, mut rx) = Logs::channel(Metrics::new("edge-1").unwrap(), "edge-1".into());
    let mut http_shutdown = shutdown.clone();
    let session_events = events.clone();
    let (sent, received) = tokio::sync::oneshot::channel();
    let connections = tokio::spawn(async move {
        shutdown.changed().await.unwrap();
        session_events.event("connection_closed", json!({}));
    });
    let exporter = tokio::spawn(async move {
        let mut count = 0;
        while rx.recv().await.is_some() {
            count += 1;
        }
        sent.send(count).unwrap();
    });
    let status = crate::supervise(
        connections,
        tokio::spawn(async move {
            http_shutdown.changed().await.unwrap();
            Ok(())
        }),
        exporter,
        stop,
        events,
        async {},
    )
    .await;
    assert_eq!(status, std::process::ExitCode::SUCCESS);
    // service_stopping and the final session event reached the exporter.
    assert_eq!(received.await.unwrap(), 2);
}

#[tokio::test(start_paused = true)]
async fn requested_signal_bounds_stalled_cleanup_to_one_deadline() {
    let (stop, _shutdown) = watch::channel(None);
    let (events, _rx) = Logs::channel(Metrics::new("edge-1").unwrap(), "edge-1".into());
    let started = tokio::time::Instant::now();
    let status = crate::supervise(
        tokio::spawn(std::future::pending()),
        tokio::spawn(std::future::pending()),
        tokio::spawn(std::future::pending()),
        stop,
        events,
        async {},
    )
    .await;
    assert_eq!(status, std::process::ExitCode::SUCCESS);
    assert_eq!(
        tokio::time::Instant::now() - started,
        Duration::from_secs(10)
    );
}

#[tokio::test]
async fn abort_exporter_accounts_queued_and_inflight_events() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/push", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let (accepted, received) = tokio::sync::oneshot::channel();
    let receiver = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = [0u8; 1024];
        assert!(stream.read(&mut bytes).await.unwrap() > 0);
        let _ = accepted.send(());
        std::future::pending::<()>().await;
    });
    let metrics = Metrics::new("edge-1").unwrap();
    let (events, rx) = Logs::channel(Arc::clone(&metrics), "edge-1".into());
    for _ in 0..300 {
        events.event("service_started", json!({}));
    }
    let (_stop, shutdown) = watch::channel(None);
    let exporter = tokio::spawn(logs::export(
        rx,
        metrics.clone(),
        "edge-1".into(),
        url,
        logs::client().unwrap(),
        shutdown,
    ));
    tokio::time::timeout(Duration::from_secs(2), received)
        .await
        .unwrap()
        .unwrap();
    exporter.abort();
    let _ = exporter.await;
    assert_eq!(metrics.queue.get(), 0);
    assert_eq!(metrics.dropped.with_label_values(&["shutdown"]).get(), 300);
    receiver.abort();
    let _ = receiver.await;
}
