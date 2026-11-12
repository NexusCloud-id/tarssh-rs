use super::*;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::ReadBuf;

fn config() -> Arc<Config> {
    Arc::new(
        Config::parse(|key| match key {
            "TARSSH_ALLOY_URL" => Some("http://127.0.0.1:9/push".into()),
            "TARSSH_DEPLOYMENT" => Some("edge-1".into()),
            "TARSSH_DELAY_MS" => Some("100".into()),
            "TARSSH_WRITE_TIMEOUT_MS" => Some("200".into()),
            "TARSSH_MAX_CONNECTIONS" => Some("1".into()),
            "TARSSH_MAX_LINE_LENGTH" => Some("255".into()),
            _ => None,
        })
        .unwrap(),
    )
}
async fn setup<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    stream: S,
) -> (
    Arc<Metrics>,
    Arc<Semaphore>,
    tokio::sync::watch::Sender<Option<Instant>>,
    tokio::task::JoinHandle<()>,
) {
    let metrics = Metrics::new("edge-1").unwrap();
    let (logs, _rx) = Logs::channel(metrics.clone(), "edge-1".into());
    let semaphore = Arc::new(Semaphore::new(1));
    let state = Session::new(
        metrics.clone(),
        logs,
        semaphore.clone().acquire_owned().await.unwrap(),
        "test-1".into(),
        "[::ffff:127.0.0.1]:1234".parse().unwrap(),
        2222,
    );
    let (stop, shutdown) = tokio::sync::watch::channel(None);
    let task = tokio::spawn(session(stream, config(), state, shutdown));
    tokio::task::yield_now().await;
    (metrics, semaphore, stop, task)
}

#[tokio::test(start_paused = true)]
async fn full_initial_delay_and_no_catchup_bursts() {
    let (mut peer, stream) = tokio::io::duplex(4096);
    let (metrics, semaphore, stop, task) = setup(stream).await;
    tokio::time::advance(Duration::from_millis(99)).await;
    tokio::task::yield_now().await;
    assert_eq!(metrics.bytes.get(), 0);
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(metrics.lines.get(), 1);
    let mut buffer = [0u8; 512];
    let n = peer.read(&mut buffer).await.unwrap();
    assert!(buffer[..n].ends_with(b"\r\n"));
    assert_eq!(metrics.bytes.get(), n as u64);
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    assert_eq!(metrics.lines.get(), 2);
    stop.send(Some(Instant::now() + Duration::from_secs(10)))
        .unwrap();
    task.await.unwrap();
    assert_eq!(metrics.closed.with_label_values(&["shutdown"]).get(), 1);
    assert_eq!(metrics.active.get(), 0);
    assert_eq!(semaphore.available_permits(), 1);
}

#[tokio::test(start_paused = true)]
async fn eof_and_input_limit_interrupt_initial_sleep() {
    let (peer, stream) = tokio::io::duplex(16384);
    let (metrics, _, _stop, task) = setup(stream).await;
    drop(peer);
    task.await.unwrap();
    assert_eq!(metrics.closed.with_label_values(&["peer_closed"]).get(), 1);
    assert_eq!(metrics.bytes.get(), 0);
    let (mut peer, stream) = tokio::io::duplex(16384);
    let (metrics, _, _stop, task) = setup(stream).await;
    peer.write_all(&vec![b'x'; 8192]).await.unwrap();
    tokio::task::yield_now().await;
    assert_eq!(metrics.active.get(), 1);
    peer.write_all(b"x").await.unwrap();
    task.await.unwrap();
    assert_eq!(metrics.closed.with_label_values(&["input_limit"]).get(), 1);
    assert_eq!(metrics.bytes.get(), 0);
}

struct Scripted {
    written: usize,
    read_error: bool,
    write_error: bool,
}
impl AsyncRead for Scripted {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.read_error {
            Poll::Ready(Err(io::Error::other("read")))
        } else {
            Poll::Pending
        }
    }
}
impl AsyncWrite for Scripted {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.written == 0 {
            self.written += 1;
            return Poll::Ready(Ok(bytes.len().min(1)));
        }
        if self.write_error {
            Poll::Ready(Err(io::Error::other("write")))
        } else {
            Poll::Pending
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
#[tokio::test(start_paused = true)]
async fn partial_write_timeout_counts_actual_bytes() {
    let (metrics, _, _stop, task) = setup(Scripted {
        written: 0,
        read_error: false,
        write_error: false,
    })
    .await;
    task.await.unwrap();
    assert_eq!(metrics.bytes.get(), 1);
    assert_eq!(metrics.lines.get(), 0);
    assert_eq!(
        metrics.closed.with_label_values(&["write_timeout"]).get(),
        1
    );
    assert!(metrics.duration.get_sample_sum() >= 0.3);
}
#[tokio::test(start_paused = true)]
async fn partial_write_and_read_errors_classified() {
    let (metrics, _, _stop, task) = setup(Scripted {
        written: 0,
        read_error: false,
        write_error: true,
    })
    .await;
    task.await.unwrap();
    assert_eq!(metrics.bytes.get(), 1);
    assert_eq!(metrics.closed.with_label_values(&["write_error"]).get(), 1);
    let (metrics, _, _stop, task) = setup(Scripted {
        written: 0,
        read_error: true,
        write_error: false,
    })
    .await;
    task.await.unwrap();
    assert_eq!(metrics.closed.with_label_values(&["read_error"]).get(), 1);
    assert_eq!(metrics.bytes.get(), 0);
}
#[tokio::test(start_paused = true)]
async fn shutdown_and_eof_interrupt_blocked_partial_write() {
    let (metrics, _, stop, task) = setup(Scripted {
        written: 0,
        read_error: false,
        write_error: false,
    })
    .await;
    tokio::time::advance(Duration::from_millis(100)).await;
    tokio::task::yield_now().await;
    stop.send(Some(Instant::now() + Duration::from_secs(10)))
        .unwrap();
    task.await.unwrap();
    assert_eq!(metrics.bytes.get(), 1);
    assert_eq!(metrics.closed.with_label_values(&["shutdown"]).get(), 1);
    let (mut peer, stream) = tokio::io::duplex(1);
    let (metrics, _, _stop, task) = setup(stream).await;
    tokio::time::advance(Duration::from_millis(100)).await;
    tokio::task::yield_now().await;
    peer.shutdown().await.unwrap();
    task.await.unwrap();
    assert_eq!(metrics.bytes.get(), 1);
    assert_eq!(metrics.closed.with_label_values(&["peer_closed"]).get(), 1);
}
#[tokio::test]
async fn aborted_and_panicking_tasks_release_gauge_and_permit() {
    let (_peer, stream) = tokio::io::duplex(1);
    let (metrics, permits, _stop, task) = setup(stream).await;
    task.abort();
    let _ = task.await;
    assert_eq!(metrics.active.get(), 0);
    assert_eq!(permits.available_permits(), 1);
    assert_eq!(
        metrics.closed.with_label_values(&["internal_error"]).get(),
        1
    );
    let (logs, _rx) = Logs::channel(metrics.clone(), "edge-1".into());
    let guard = Session::new(
        metrics.clone(),
        logs,
        permits.clone().acquire_owned().await.unwrap(),
        "test-2".into(),
        "127.0.0.1:1234".parse().unwrap(),
        2222,
    );
    let task = tokio::spawn(async move {
        let _guard = guard;
        panic!("session panic")
    });
    assert!(task.await.is_err());
    assert_eq!(metrics.active.get(), 0);
    assert_eq!(permits.available_permits(), 1);
    assert_eq!(metrics.duration.get_sample_count(), 2);
}
#[tokio::test]
async fn capacity_cleanup_and_progress_during_exporter_outage() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Metrics::new("edge-1").unwrap();
    let (logs, rx) = Logs::channel(metrics.clone(), "edge-1".into());
    let (stop, shutdown) = tokio::sync::watch::channel(None);
    let export = tokio::spawn(crate::logs::export(
        rx,
        metrics.clone(),
        "edge-1".into(),
        config().alloy.clone(),
        crate::logs::client().unwrap(),
        shutdown.clone(),
    ));
    let server = tokio::spawn(serve(listener, config(), metrics.clone(), logs, shutdown));
    let mut peer = tokio::net::TcpStream::connect(addr).await.unwrap();
    while metrics.active.get() == 0 {
        tokio::task::yield_now().await;
    }
    let mut excess = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut buf = [0; 1024];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), excess.read(&mut buf))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert_eq!(metrics.rejected.get(), 1);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), peer.read(&mut buf))
            .await
            .unwrap()
            .unwrap()
            > 0
    );
    drop(peer);
    while metrics.active.get() != 0 {
        tokio::task::yield_now().await;
    }
    let peer = tokio::net::TcpStream::connect(addr).await.unwrap();
    while metrics.accepted.get() != 2 {
        tokio::task::yield_now().await;
    }
    stop.send(Some(Instant::now() + Duration::from_millis(100)))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
    drop(peer);
    tokio::time::timeout(Duration::from_secs(1), export)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metrics.active.get(), 0);
    assert_eq!(metrics.closed.with_label_values(&["shutdown"]).get(), 1);
    assert_eq!(metrics.queue.get(), 0);
}
