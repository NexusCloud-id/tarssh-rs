use super::*;
use axum::{Router, body::Bytes, http::StatusCode, routing::post};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

async fn receiver(
    codes: Vec<u16>,
    retry_after: Option<&'static str>,
) -> (
    reqwest::Url,
    Arc<Mutex<Vec<Value>>>,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let b = bodies.clone();
    let c = calls.clone();
    let app = Router::new().route(
        "/push",
        post(move |body: Bytes| {
            let n = c.fetch_add(1, Ordering::SeqCst);
            b.lock()
                .unwrap()
                .push(serde_json::from_slice(&body).unwrap());
            let code = codes[n.min(codes.len() - 1)];
            async move {
                (
                    StatusCode::from_u16(code).unwrap(),
                    [("retry-after", retry_after.unwrap_or("0"))],
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/push", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, bodies, calls, task)
}

#[tokio::test]
async fn queue_bounds_oversize_closed_and_gauge_ownership() {
    let metrics = Metrics::new("edge-1").unwrap();
    let (logs, mut rx) = Logs::channel(metrics.clone(), "edge-1".into());
    logs.enqueue("x".repeat(EVENT_LIMIT + 1));
    assert_eq!(metrics.dropped.with_label_values(&["oversize"]).get(), 1);
    for _ in 0..QUEUE_CAPACITY + 1 {
        logs.event("service_started", json!({}));
    }
    assert_eq!(metrics.queue.get(), QUEUE_CAPACITY as i64);
    assert_eq!(metrics.dropped.with_label_values(&["queue_full"]).get(), 1);
    let event: Value = serde_json::from_str(&rx.recv().await.unwrap().dequeue()).unwrap();
    assert_eq!(event["schema_version"], 1);
    assert_eq!(event["event"], "service_started");
    assert_eq!(event["deployment"], "edge-1");
    assert!(
        event["timestamp_ns"]
            .as_str()
            .unwrap()
            .parse::<u128>()
            .is_ok()
    );
    drop(rx);
    assert_eq!(metrics.queue.get(), 0);
    logs.event("service_started", json!({}));
    assert_eq!(
        metrics.dropped.with_label_values(&["shutdown"]).get(),
        QUEUE_CAPACITY as u64
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_queue_does_not_leak_or_go_negative() {
    let metrics = Metrics::new("edge-1").unwrap();
    let (logs, mut rx) = Logs::channel(metrics.clone(), "edge-1".into());
    let consumer_metrics = metrics.clone();
    let consumer = tokio::spawn(async move {
        while let Some(entry) = rx.recv().await {
            drop(entry);
            assert!(consumer_metrics.queue.get() >= 0);
        }
    });
    let mut producers = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let logs = logs.clone();
        producers.spawn(async move {
            for _ in 0..4000 {
                logs.event("service_started", json!({}));
            }
        });
    }
    while let Some(result) = producers.join_next().await {
        result.unwrap();
    }
    drop(logs);
    consumer.await.unwrap();
    assert_eq!(metrics.queue.get(), 0);
}

#[tokio::test]
async fn batches_fifo_json_labels_and_monotonic_timestamps() {
    let (url, bodies, calls, server) = receiver(vec![204], None).await;
    let metrics = Metrics::new("edge-1").unwrap();
    let (logs, rx) = Logs::channel(metrics.clone(), "edge-1".into());
    for n in 0..260 {
        logs.event("service_started", json!({"n":n,"timestamp_ns":"1"}));
    }
    drop(logs);
    let (_stop, shutdown) = watch::channel(None);
    export(
        rx,
        metrics.clone(),
        "edge-1".into(),
        url,
        client().unwrap(),
        shutdown,
    )
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(metrics.sent.get(), 260);
    assert_eq!(metrics.queue.get(), 0);
    let bodies = bodies.lock().unwrap().clone();
    let mut previous = 0;
    let mut index = 0;
    for body in bodies.iter() {
        let stream = &body["streams"][0];
        assert_eq!(
            stream["stream"],
            json!({"service_name":"tarssh-rs","deployment":"edge-1"})
        );
        let values = stream["values"].as_array().unwrap();
        assert!(values.len() <= 128);
        for pair in values {
            let timestamp = pair[0].as_str().unwrap().parse::<u128>().unwrap();
            assert!(timestamp > previous);
            previous = timestamp;
            let event: Value = serde_json::from_str(pair[1].as_str().unwrap()).unwrap();
            assert_eq!(event["n"], index);
            assert_eq!(event["timestamp_ns"], "1");
            index += 1;
        }
    }
    drop(bodies);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn retries_exactly_three_with_no_hidden_retries() {
    for code in [408, 429, 503] {
        let (url, _, calls, server) = receiver(vec![code], None).await;
        let metrics = Metrics::new("edge-1").unwrap();
        let (_stop, mut shutdown) = watch::channel(None);
        assert_eq!(
            push(
                &client().unwrap(),
                &url,
                "{}".into(),
                &metrics,
                &mut shutdown
            )
            .await,
            Err("retry_exhausted")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let reason = match code {
            408 => "timeout",
            429 => "rate_limited",
            _ => "server_error",
        };
        assert_eq!(metrics.failures.with_label_values(&[reason]).get(), 3);
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn success_recovery_and_terminal_statuses() {
    for code in [200, 204, 260, 302, 400, 401] {
        let (url, _, calls, server) = receiver(vec![code], None).await;
        let metrics = Metrics::new("edge-1").unwrap();
        let (_stop, mut shutdown) = watch::channel(None);
        let result = push(
            &client().unwrap(),
            &url,
            "{}".into(),
            &metrics,
            &mut shutdown,
        )
        .await;
        assert_eq!(
            result,
            if code == 200 || code == 204 {
                Ok(())
            } else {
                Err("permanent_error")
            }
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        server.abort();
        let _ = server.await;
    }
    let (url, _, calls, server) = receiver(vec![503, 429, 204], None).await;
    let metrics = Metrics::new("edge-1").unwrap();
    let (_stop, mut shutdown) = watch::channel(None);
    assert!(
        push(
            &client().unwrap(),
            &url,
            "{}".into(),
            &metrics,
            &mut shutdown
        )
        .await
        .is_ok()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn shutdown_interrupts_retry_backoff_counts_batch_and_queue() {
    let (url, _, calls, server) = receiver(vec![429], Some("999999999")).await;
    let metrics = Metrics::new("edge-1").unwrap();
    let (logs, rx) = Logs::channel(metrics.clone(), "edge-1".into());
    for _ in 0..300 {
        logs.event("service_started", json!({}));
    }
    drop(logs);
    let (stop, shutdown) = watch::channel(None);
    let task = tokio::spawn(export(
        rx,
        metrics.clone(),
        "edge-1".into(),
        url,
        client().unwrap(),
        shutdown,
    ));
    while calls.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    stop.send(Some(Instant::now() + Duration::from_millis(30)))
        .unwrap();
    tokio::time::timeout(Duration::from_millis(500), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metrics.dropped.with_label_values(&["shutdown"]).get(), 300);
    assert_eq!(metrics.queue.get(), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn flush_interval_without_closed_producers() {
    let (url, _, calls, server) = receiver(vec![204], None).await;
    let metrics = Metrics::new("edge-1").unwrap();
    let (logs, rx) = Logs::channel(metrics.clone(), "edge-1".into());
    let (_stop, shutdown) = watch::channel(None);
    logs.event("service_started", json!({}));
    let task = tokio::spawn(export(
        rx,
        metrics.clone(),
        "edge-1".into(),
        url,
        client().unwrap(),
        shutdown,
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(logs);
    task.await.unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn hard_batch_deadline_clamps_retry_after() {
    let (url, _, calls, server) = receiver(vec![429], Some("999999999")).await;
    let metrics = Metrics::new("edge-1").unwrap();
    let (_stop, mut shutdown) = watch::channel(None);
    let http = client().unwrap();
    let start = Instant::now();
    assert_eq!(
        push(&http, &url, "{}".into(), &metrics, &mut shutdown).await,
        Err("retry_exhausted")
    );
    assert!(start.elapsed() >= Duration::from_secs(7));
    assert!(start.elapsed() < Duration::from_secs(9));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn shutdown_cancels_inflight_request() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/push", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let server = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    let metrics = Metrics::new("edge-1").unwrap();
    let (stop, mut shutdown) = watch::channel(None);
    let cloned = metrics.clone();
    let task = tokio::spawn(async move {
        push(
            &client().unwrap(),
            &url,
            "{}".into(),
            &cloned,
            &mut shutdown,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    stop.send(Some(Instant::now() + Duration::from_millis(20)))
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(300), task)
            .await
            .unwrap()
            .unwrap(),
        Err("shutdown")
    );
    assert_eq!(metrics.failures.with_label_values(&["timeout"]).get(), 1);
    server.abort();
    let _ = server.await;
}
