use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

struct Running(Child);
impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
fn command() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_tarssh-rs"));
    for key in [
        "TARSSH_LISTEN_ADDR",
        "TARSSH_METRICS_ADDR",
        "TARSSH_DELAY_MS",
        "TARSSH_MAX_LINE_LENGTH",
        "TARSSH_MAX_CONNECTIONS",
        "TARSSH_WRITE_TIMEOUT_MS",
        "TARSSH_ALLOY_URL",
        "TARSSH_DEPLOYMENT",
    ] {
        cmd.env_remove(key);
    }
    cmd
}
fn connect(addr: SocketAddr) -> TcpStream {
    let start = Instant::now();
    loop {
        if let Ok(stream) = TcpStream::connect(addr) {
            return stream;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "service failed to bind {addr}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}
#[test]
fn startup_failure_is_one_redacted_diagnostic() {
    let result = command()
        .env("TARSSH_ALLOY_URL", "http://user:do-not-leak@localhost/push")
        .env("TARSSH_DEPLOYMENT", "edge-1")
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(result.stdout.is_empty());
    let stderr = String::from_utf8(result.stderr).unwrap();
    assert_eq!(stderr.lines().count(), 1);
    assert!(!stderr.contains("do-not-leak"));
}
#[test]
fn binary_serves_metrics_and_tarpit_despite_collector_outage() {
    let listen = free_addr();
    let metrics = free_addr();
    let unreachable = free_addr();
    let child = command()
        .env("TARSSH_LISTEN_ADDR", listen.to_string())
        .env("TARSSH_METRICS_ADDR", metrics.to_string())
        .env("TARSSH_DELAY_MS", "50")
        .env("TARSSH_ALLOY_URL", format!("http://{unreachable}/push"))
        .env("TARSSH_DEPLOYMENT", "edge-1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut process = Running(child);
    let mut peer = connect(listen);
    peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let mut line = [0; 255];
    let n = peer.read(&mut line).unwrap();
    assert!(line[..n].ends_with(b"\r\n"));
    let mut scrape = connect(metrics);
    scrape
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    scrape
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut response = String::new();
    scrape.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(
        response
            .lines()
            .any(|line| line.starts_with("tarssh_connections_active{")
                && line.contains("deployment=\"edge-1\"")
                && line.contains("service_name=\"tarssh-rs\"")
                && line.ends_with(" 1")),
        "{response}"
    );
    #[cfg(unix)]
    {
        assert!(
            Command::new("kill")
                .arg("-TERM")
                .arg(process.0.id().to_string())
                .status()
                .unwrap()
                .success()
        );
        let start = Instant::now();
        loop {
            if let Some(status) = process.0.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            assert!(start.elapsed() < Duration::from_secs(11));
            thread::sleep(Duration::from_millis(20));
        }
        let mut stdout = String::new();
        let mut stderr = String::new();
        process
            .0
            .stdout
            .as_mut()
            .unwrap()
            .read_to_string(&mut stdout)
            .unwrap();
        process
            .0
            .stderr
            .as_mut()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        assert!(stdout.is_empty());
        assert!(stderr.is_empty());
    }
}
