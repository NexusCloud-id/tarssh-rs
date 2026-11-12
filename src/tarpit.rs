use rand::RngExt;

pub fn line(max: usize) -> Vec<u8> {
    let length = rand::rng().random_range(3..=max);
    let mut bytes = vec![0; length - 2];
    for byte in &mut bytes {
        *byte = rand::rng().random_range(b' '..=b'~');
    }
    if bytes.starts_with(b"SSH-") {
        bytes[0] = b'X';
    }
    bytes.extend_from_slice(b"\r\n");
    bytes
}

use crate::{
    config::Config,
    logs::{Logs, Shutdown},
    metrics::Metrics,
};
use serde_json::json;
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::{Duration, Instant},
};

struct Session {
    metrics: Arc<Metrics>,
    logs: Logs,
    _permit: OwnedSemaphorePermit,
    fields: serde_json::Value,
    started: Instant,
    bytes: u64,
    lines: u64,
    reason: &'static str,
}
impl Session {
    fn new(
        metrics: Arc<Metrics>,
        logs: Logs,
        permit: OwnedSemaphorePermit,
        id: String,
        peer: SocketAddr,
        local: u16,
    ) -> Self {
        let peer_ip = match peer.ip() {
            std::net::IpAddr::V6(ip) => ip
                .to_ipv4_mapped()
                .map(std::net::IpAddr::V4)
                .unwrap_or(std::net::IpAddr::V6(ip)),
            ip => ip,
        };
        let fields = json!({"connection_id":id,"peer_ip":peer_ip.to_string(),"peer_port":peer.port(),"local_port":local});
        metrics.active.inc();
        metrics.accepted.inc();
        logs.event("connection_opened", fields.clone());
        Self {
            metrics,
            logs,
            _permit: permit,
            fields,
            started: Instant::now(),
            bytes: 0,
            lines: 0,
            reason: "internal_error",
        }
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        let duration = self.started.elapsed().as_secs_f64();
        self.metrics.active.dec();
        self.metrics.closed.with_label_values(&[self.reason]).inc();
        self.metrics.duration.observe(duration);
        let mut fields = self.fields.clone();
        fields.as_object_mut().expect("literal object").extend(json!({"duration_seconds":duration,"bytes_sent":self.bytes,"lines_sent":self.lines,"reason":self.reason}).as_object().expect("literal object").clone());
        self.logs.event("connection_closed", fields);
    }
}

async fn session<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    config: Arc<Config>,
    mut state: Session,
    mut shutdown: Shutdown,
) {
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut buffer = [0u8; 1024];
    let mut received = 0usize;
    let input = async {
        loop {
            match reader.read(&mut buffer).await {
                Ok(0) => return "peer_closed",
                Ok(n) => {
                    received += n;
                    if received > 8192 {
                        return "input_limit";
                    }
                }
                Err(_) => return "read_error",
            }
            tokio::task::yield_now().await;
        }
    };
    let output = async {
        loop {
            tokio::time::sleep(config.delay).await;
            let bytes = line(config.max_line);
            let write = async {
                let mut offset = 0;
                while offset < bytes.len() {
                    match writer.write(&bytes[offset..]).await {
                        Ok(0) => return Err(()),
                        Ok(n) => {
                            offset += n;
                            state.bytes += n as u64;
                            state.metrics.bytes.inc_by(n as u64);
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(_) => return Err(()),
                    }
                }
                Ok(())
            };
            match tokio::time::timeout(config.write_timeout, write).await {
                Err(_) => return "write_timeout",
                Ok(Err(())) => return "write_error",
                Ok(Ok(())) => {
                    state.lines += 1;
                    state.metrics.lines.inc();
                }
            }
        }
    };
    let reason = tokio::select! {
        biased;
        _ = async { if shutdown.borrow().is_none() { let _ = shutdown.changed().await; } } => "shutdown",
        reason = input => reason,
        reason = output => reason,
    };
    state.reason = reason;
}

pub async fn serve(
    listener: TcpListener,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
    logs: Logs,
    mut shutdown: Shutdown,
) {
    let permits = Arc::new(Semaphore::new(config.max_connections));
    let token = format!(
        "{:032x}-{:032x}",
        crate::logs::wall_ns(),
        rand::random::<u128>()
    );
    let mut number = 0u64;
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = async { if shutdown.borrow().is_none() { let _ = shutdown.changed().await; } } => break,
            _ = tasks.join_next(), if !tasks.is_empty() => {},
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    let Ok(permit) = permits.clone().try_acquire_owned() else {
                        metrics.rejected.inc();
                        continue;
                    };
                    let local = stream.local_addr().map_or(config.listen.port(), |a| a.port());
                    number += 1;
                    let state = Session::new(metrics.clone(), logs.clone(), permit, format!("{token}-{number}"), peer, local);
                    tasks.spawn(session(stream, config.clone(), state, shutdown.clone()));
                }
                Err(_) => {
                    metrics.accept_errors.inc();
                    tokio::select! {
                        _ = shutdown.changed() => break,
                        _ = tokio::time::sleep(Duration::from_millis(100)) => {},
                    }
                }
            }
        }
    }
    drop(listener);
    let end = shutdown
        .borrow()
        .unwrap_or_else(|| Instant::now() + Duration::from_secs(10));
    while !tasks.is_empty() {
        if tokio::time::timeout_at(end, tasks.join_next())
            .await
            .is_err()
        {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            break;
        }
    }
}

#[cfg(test)]
#[path = "tarpit_tests.rs"]
mod lifecycle_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn randomized_wire_contract() {
        for max in 3..=255 {
            for _ in 0..100 {
                let bytes = line(max);
                assert!((3..=max).contains(&bytes.len()));
                assert!(bytes.ends_with(b"\r\n"));
                assert!(!bytes.starts_with(b"SSH-"));
                assert!(
                    bytes[..bytes.len() - 2]
                        .iter()
                        .all(|b| (32..=126).contains(b))
                );
            }
        }
    }
}
