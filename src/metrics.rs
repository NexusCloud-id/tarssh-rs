use axum::{Router, http::header, routing::get};
use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
    TextEncoder,
};
use std::{collections::HashMap, sync::Arc};

pub const CLOSE_REASONS: &[&str] = &[
    "peer_closed",
    "read_error",
    "write_error",
    "write_timeout",
    "input_limit",
    "shutdown",
    "internal_error",
];
pub const DROP_REASONS: &[&str] = &[
    "queue_full",
    "oversize",
    "permanent_error",
    "retry_exhausted",
    "shutdown",
];
pub const PUSH_REASONS: &[&str] = &[
    "transport",
    "timeout",
    "rate_limited",
    "server_error",
    "client_error",
];

pub struct Metrics {
    pub registry: Registry,
    pub active: IntGauge,
    pub accepted: IntCounter,
    pub rejected: IntCounter,
    pub closed: IntCounterVec,
    pub duration: Histogram,
    pub bytes: IntCounter,
    pub lines: IntCounter,
    pub accept_errors: IntCounter,
    pub queue: IntGauge,
    pub sent: IntCounter,
    pub dropped: IntCounterVec,
    pub failures: IntCounterVec,
}

impl Metrics {
    pub fn new(deployment: &str) -> Result<Arc<Self>, prometheus::Error> {
        let registry = Registry::new_custom(
            None,
            Some(HashMap::from([
                ("service_name".into(), "tarssh-rs".into()),
                ("deployment".into(), deployment.into()),
            ])),
        )?;
        let counter = |name: &str, help: &str| -> Result<IntCounter, prometheus::Error> {
            let metric = IntCounter::new(format!("tarssh_{name}"), help)?;
            registry.register(Box::new(metric.clone()))?;
            Ok(metric)
        };
        let gauge = |name: &str, help: &str| -> Result<IntGauge, prometheus::Error> {
            let metric = IntGauge::new(format!("tarssh_{name}"), help)?;
            registry.register(Box::new(metric.clone()))?;
            Ok(metric)
        };
        let vector = |name: &str,
                      help: &str,
                      reasons: &[&str]|
         -> Result<IntCounterVec, prometheus::Error> {
            let metric =
                IntCounterVec::new(Opts::new(format!("tarssh_{name}"), help), &["reason"])?;
            for reason in reasons {
                metric.with_label_values(&[reason]);
            }
            registry.register(Box::new(metric.clone()))?;
            Ok(metric)
        };
        let duration = Histogram::with_opts(
            HistogramOpts::new(
                "tarssh_connection_duration_seconds",
                "Completed session duration.",
            )
            .buckets(vec![
                1., 10., 60., 300., 900., 3600., 21600., 86400., 604800.,
            ]),
        )?;
        registry.register(Box::new(duration.clone()))?;
        Ok(Arc::new(Self {
            active: gauge("connections_active", "Admitted active sessions.")?,
            accepted: counter("connections_accepted_total", "Admitted sessions.")?,
            rejected: counter(
                "connections_rejected_total",
                "Sessions rejected at capacity.",
            )?,
            closed: vector(
                "connections_closed_total",
                "Completed sessions by reason.",
                CLOSE_REASONS,
            )?,
            duration,
            bytes: counter("bytes_sent_total", "Successfully written wire bytes.")?,
            lines: counter("lines_sent_total", "Completely written lines.")?,
            accept_errors: counter("accept_errors_total", "Listener accept failures.")?,
            queue: gauge("log_queue_events", "Events waiting in the export channel.")?,
            sent: counter(
                "log_events_sent_total",
                "Events accepted by Alloy, not necessarily persisted.",
            )?,
            dropped: vector(
                "log_events_dropped_total",
                "Events discarded before delivery.",
                DROP_REASONS,
            )?,
            failures: vector(
                "log_push_failures_total",
                "Failed HTTP delivery attempts.",
                PUSH_REASONS,
            )?,
            registry,
        }))
    }
    pub fn encode(&self) -> Result<Vec<u8>, prometheus::Error> {
        let mut bytes = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut bytes)?;
        Ok(bytes)
    }
    pub fn router(self: &Arc<Self>) -> Router {
        let metrics = self.clone();
        Router::new().route(
            "/metrics",
            get(move || async move {
                metrics
                    .encode()
                    .map(|body| {
                        (
                            [(
                                header::CONTENT_TYPE,
                                TextEncoder::new().format_type().to_string(),
                            )],
                            body,
                        )
                    })
                    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)
            }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn initialized_metrics_contract() {
        let metrics = Metrics::new("edge-1").unwrap();
        let text = String::from_utf8(metrics.encode().unwrap()).unwrap();
        for family in metrics.registry.gather() {
            for metric in family.get_metric() {
                let labels = metric.get_label();
                assert!(
                    labels
                        .iter()
                        .any(|l| l.name() == "service_name" && l.value() == "tarssh-rs")
                );
                assert!(
                    labels
                        .iter()
                        .any(|l| l.name() == "deployment" && l.value() == "edge-1")
                );
                assert!(
                    labels
                        .iter()
                        .all(|l| matches!(l.name(), "service_name" | "deployment" | "reason"))
                );
            }
        }
        for (name, reasons) in [
            ("connections_closed", CLOSE_REASONS),
            ("log_events_dropped", DROP_REASONS),
            ("log_push_failures", PUSH_REASONS),
        ] {
            for reason in reasons {
                assert!(text.lines().any(
                    |line| line.starts_with(&format!("tarssh_{name}_total{{"))
                        && line.contains(&format!("reason=\"{reason}\""))
                        && line.ends_with(" 0")
                ));
            }
        }
        for boundary in [
            "1", "10", "60", "300", "900", "3600", "21600", "86400", "604800", "+Inf",
        ] {
            assert!(text.contains(&format!("le=\"{boundary}\"")));
        }
        assert_eq!(metrics.registry.gather().len(), 12);
        assert!(text.contains("tarssh_connections_active{"));
        assert!(text.contains("tarssh_connection_duration_seconds_count{"));
    }
    #[tokio::test]
    async fn http_content_type_and_unknown_route() {
        let metrics = Metrics::new("edge-1").unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task =
            tokio::spawn(async move { axum::serve(listener, metrics.router()).await.unwrap() });
        let client = reqwest::Client::new();
        let response = client
            .get(format!("http://{address}/metrics"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            TextEncoder::new().format_type()
        );
        assert!(
            response
                .text()
                .await
                .unwrap()
                .contains("tarssh_connections_active")
        );
        assert_eq!(
            client
                .get(format!("http://{address}/unknown"))
                .send()
                .await
                .unwrap()
                .status(),
            404
        );
        task.abort();
        let _ = task.await;
    }
}
