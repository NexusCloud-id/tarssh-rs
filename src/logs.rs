use crate::metrics::Metrics;
use serde_json::{Value, json};
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};

const QUEUE_CAPACITY: usize = 4096;
const EVENT_LIMIT: usize = 2048;
const BATCH_COUNT: usize = 128;
const BATCH_BYTES: usize = 256 * 1024;
const BATCH_BUDGET: Duration = Duration::from_secs(8);
pub type Shutdown = watch::Receiver<Option<Instant>>;

pub fn wall_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

// An entry owns its gauge contribution, including receiver cancellation/drop.
pub struct Entry {
    json: String,
    metrics: Arc<Metrics>,
}
impl Drop for Entry {
    fn drop(&mut self) {
        self.metrics.queue.dec();
        if !self.json.is_empty() {
            self.metrics.dropped.with_label_values(&["shutdown"]).inc();
        }
    }
}
impl Entry {
    fn dequeue(mut self) -> String {
        std::mem::take(&mut self.json)
    }
}

#[derive(Clone)]
pub struct Logs {
    tx: mpsc::Sender<Entry>,
    metrics: Arc<Metrics>,
    deployment: String,
}
impl Logs {
    pub fn channel(metrics: Arc<Metrics>, deployment: String) -> (Self, mpsc::Receiver<Entry>) {
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        (
            Self {
                tx,
                metrics,
                deployment,
            },
            rx,
        )
    }
    pub fn event(&self, name: &str, fields: Value) {
        let mut event = json!({"schema_version":1, "timestamp_ns":wall_ns().to_string(), "level":"info", "event":name, "service_name":"tarssh-rs", "deployment":self.deployment});
        if let Some(fields) = fields.as_object() {
            event
                .as_object_mut()
                .expect("literal object")
                .extend(fields.clone());
        }
        let Ok(serialized) = serde_json::to_string(&event) else {
            return;
        };
        self.enqueue(serialized);
    }
    fn enqueue(&self, serialized: String) {
        if serialized.len() > EVENT_LIMIT {
            self.metrics.dropped.with_label_values(&["oversize"]).inc();
            return;
        }
        match self.tx.try_reserve() {
            Ok(permit) => {
                self.metrics.queue.inc();
                permit.send(Entry {
                    json: serialized,
                    metrics: self.metrics.clone(),
                });
            }
            Err(mpsc::error::TrySendError::Full(_)) => self
                .metrics
                .dropped
                .with_label_values(&["queue_full"])
                .inc(),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.dropped.with_label_values(&["shutdown"]).inc()
            }
        }
    }
}

pub fn client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(2))
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_proxy()
        .build()
}

fn deadline(limit: Instant, shutdown: &Shutdown) -> Instant {
    shutdown.borrow().map_or(limit, |end| end.min(limit))
}

// Keep a request alive across shutdown notification, but shorten its budget.
async fn bounded<T>(
    future: impl Future<Output = T>,
    limit: Instant,
    shutdown: &mut Shutdown,
) -> Result<T, ()> {
    tokio::pin!(future);
    loop {
        let stop = deadline(limit, shutdown);
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(stop) => return Err(()),
            result = &mut future => return Ok(result),
            changed = shutdown.changed() => {
                if changed.is_err() { return Err(()); }
            }
        }
    }
}

struct BatchAccounting {
    count: u64,
    metrics: Arc<Metrics>,
}
impl Drop for BatchAccounting {
    fn drop(&mut self) {
        if self.count != 0 {
            self.metrics
                .dropped
                .with_label_values(&["shutdown"])
                .inc_by(self.count);
        }
    }
}

pub async fn export(
    mut rx: mpsc::Receiver<Entry>,
    metrics: Arc<Metrics>,
    deployment: String,
    url: reqwest::Url,
    client: reqwest::Client,
    mut shutdown: Shutdown,
) {
    let mut previous = 0u128;
    loop {
        if shutdown.borrow().is_some_and(|end| Instant::now() >= end) {
            rx.close();
            while let Ok(entry) = rx.try_recv() {
                drop(entry);
            }
            return;
        }
        let first = match bounded(
            rx.recv(),
            Instant::now() + Duration::from_secs(86400),
            &mut shutdown,
        )
        .await
        {
            Ok(Some(entry)) => entry,
            Ok(None) => return,
            Err(()) => continue,
        };
        let flush = Instant::now() + Duration::from_secs(1);
        let mut batch = Vec::with_capacity(BATCH_COUNT);
        let mut accounting = BatchAccounting {
            count: 0,
            metrics: metrics.clone(),
        };
        let mut bytes = 0;
        let mut next = Some(first);
        loop {
            if let Some(entry) = next.take() {
                let serialized = entry.dequeue();
                accounting.count += 1;
                bytes += serialized.len();
                previous = wall_ns().max(previous + 1);
                batch.push((previous.to_string(), serialized));
            }
            // 128 events * 2 KiB bounds event JSON, independently of escaping.
            if batch.len() == BATCH_COUNT || bytes + EVENT_LIMIT > BATCH_BYTES {
                break;
            }
            if shutdown.borrow().is_some() {
                match rx.try_recv() {
                    Ok(entry) => next = Some(entry),
                    Err(_) => break,
                }
            } else {
                match bounded(rx.recv(), flush, &mut shutdown).await {
                    Ok(Some(entry)) => next = Some(entry),
                    _ => break,
                }
            }
        }
        let count = batch.len() as u64;
        let payload = json!({"streams":[{"stream":{"service_name":"tarssh-rs","deployment":deployment},"values":batch}]}).to_string();
        let result = push(&client, &url, payload, &metrics, &mut shutdown).await;
        match result {
            Ok(()) => {
                metrics.sent.inc_by(count);
                accounting.count = 0;
            }
            Err(reason) => {
                metrics.dropped.with_label_values(&[reason]).inc_by(count);
                accounting.count = 0;
            }
        }
    }
}

async fn push(
    client: &reqwest::Client,
    url: &reqwest::Url,
    payload: String,
    metrics: &Metrics,
    shutdown: &mut Shutdown,
) -> Result<(), &'static str> {
    let end = Instant::now() + BATCH_BUDGET;
    for attempt in 0..3 {
        if Instant::now() >= deadline(end, shutdown) {
            break;
        }
        let response = bounded(
            client
                .post(url.clone())
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(payload.clone())
                .send(),
            end.min(Instant::now() + Duration::from_secs(2)),
            shutdown,
        )
        .await;
        let mut retry_after = Duration::ZERO;
        let (reason, retry) = match response {
            Ok(Ok(response)) => {
                let code = response.status().as_u16();
                if matches!(code, 200 | 204) {
                    return Ok(());
                }
                if let Some(seconds) = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    retry_after = Duration::from_secs(seconds.min(8));
                }
                match code {
                    408 => ("timeout", true),
                    429 => ("rate_limited", true),
                    500..=599 => ("server_error", true),
                    _ => ("client_error", false),
                }
            }
            Ok(Err(error)) if !error.is_timeout() => ("transport", true),
            _ => ("timeout", true),
        };
        metrics.failures.with_label_values(&[reason]).inc();
        if !retry {
            return Err("permanent_error");
        }
        if attempt < 2 {
            let delay =
                Duration::from_millis(if attempt == 0 { 250 } else { 1000 }).max(retry_after);
            let wake = Instant::now().checked_add(delay).unwrap_or(end).min(end);
            if bounded(tokio::time::sleep_until(wake), end, shutdown)
                .await
                .is_err()
            {
                break;
            }
        }
    }
    if shutdown.borrow().is_some_and(|stop| Instant::now() >= stop) {
        Err("shutdown")
    } else {
        Err("retry_exhausted")
    }
}

#[cfg(test)]
#[path = "logs_tests.rs"]
mod tests;
