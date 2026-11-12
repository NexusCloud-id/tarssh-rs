use crate::{
    logs::{self, Logs},
    metrics::Metrics,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{io::AsyncReadExt, net::TcpListener, sync::watch};

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
