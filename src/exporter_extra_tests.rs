use crate::{
    logs::{self, Logs},
    metrics::Metrics,
};
use axum::{Router, body::Bytes, http::StatusCode, routing::post};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::watch,
};

#[tokio::test]
async fn transport_disconnects_retry_exactly_three_times() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/push", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let n = calls.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 4096];
            assert!(socket.read(&mut buffer).await.unwrap() > 0);
            n.fetch_add(1, Ordering::SeqCst);
            socket.shutdown().await.unwrap();
        }
    });
    let metrics = Metrics::new("edge-1").unwrap();
    let (events, rx) = Logs::channel(metrics.clone(), "edge-1".into());
    events.event("service_started", json!({}));
    drop(events);
    let (_stop, shutdown) = watch::channel(None);
    logs::export(
        rx,
        metrics.clone(),
        "edge-1".into(),
        url,
        logs::client().unwrap(),
        shutdown,
    )
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(metrics.failures.with_label_values(&["transport"]).get(), 3);
    assert_eq!(
        metrics
            .dropped
            .with_label_values(&["retry_exhausted"])
            .get(),
        1
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn redirects_are_not_followed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let n = calls.clone();
    let app = Router::new().route(
        "/push",
        post(move || {
            n.fetch_add(1, Ordering::SeqCst);
            async move {
                (
                    StatusCode::TEMPORARY_REDIRECT,
                    [("location", format!("http://{addr}/push"))],
                )
            }
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let metrics = Metrics::new("edge-1").unwrap();
    let (events, rx) = Logs::channel(metrics.clone(), "edge-1".into());
    events.event("service_started", json!({}));
    drop(events);
    let (_stop, shutdown) = watch::channel(None);
    logs::export(
        rx,
        metrics.clone(),
        "edge-1".into(),
        format!("http://{addr}/push").parse().unwrap(),
        logs::client().unwrap(),
        shutdown,
    )
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        metrics
            .dropped
            .with_label_values(&["permanent_error"])
            .get(),
        1
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn shutdown_drains_successful_escaped_event_batch() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/push", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let (payload, received) = tokio::sync::oneshot::channel();
    let sender = Arc::new(std::sync::Mutex::new(Some(payload)));
    let app = Router::new().route(
        "/push",
        post(move |body: Bytes| {
            assert!(body.len() < 2 * 1024 * 1024);
            let value: Value = serde_json::from_slice(&body).unwrap();
            if let Some(sender) = sender.lock().unwrap().take() {
                let _ = sender.send(value);
            }
            async { StatusCode::NO_CONTENT }
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let metrics = Metrics::new("edge-1").unwrap();
    let (events, rx) = Logs::channel(metrics.clone(), "edge-1".into());
    for _ in 0..128 {
        events.event("service_stopping", json!({"text":"\\\"".repeat(300)}));
    }
    drop(events);
    let (_stop, shutdown) =
        watch::channel(Some(tokio::time::Instant::now() + Duration::from_secs(2)));
    logs::export(
        rx,
        metrics.clone(),
        "edge-1".into(),
        url,
        logs::client().unwrap(),
        shutdown,
    )
    .await;
    let value = received.await.unwrap();
    let values = value["streams"][0]["values"].as_array().unwrap();
    assert_eq!(values.len(), 128);
    for pair in values {
        let event: Value = serde_json::from_str(pair[1].as_str().unwrap()).unwrap();
        assert_eq!(event["text"], "\\\"".repeat(300));
    }
    assert_eq!(metrics.sent.get(), 128);
    assert_eq!(metrics.queue.get(), 0);
    assert_eq!(metrics.dropped.with_label_values(&["shutdown"]).get(), 0);
    server.abort();
    let _ = server.await;
}
