use std::{net::SocketAddr, time::Duration};

#[derive(Clone, Debug)]
pub struct Config {
    pub listen: SocketAddr,
    pub metrics: SocketAddr,
    pub delay: Duration,
    pub max_line: usize,
    pub max_connections: usize,
    pub write_timeout: Duration,
    pub alloy: reqwest::Url,
    pub deployment: String,
}

impl Config {
    pub fn parse(get: impl Fn(&str) -> Option<String>) -> Result<Self, &'static str> {
        let number = |key, default, min, max| {
            let raw = get(key).unwrap_or_else(|| format!("{default}"));
            let n = raw.parse::<u64>().map_err(|_| key)?;
            if !(min..=max).contains(&n) {
                return Err(key);
            }
            Ok(n)
        };
        let address = |key, default: &str| {
            let address = get(key)
                .unwrap_or_else(|| default.into())
                .parse::<SocketAddr>()
                .map_err(|_| key)?;
            if address.port() == 0 {
                return Err(key);
            }
            Ok(address)
        };
        let deployment = get("TARSSH_DEPLOYMENT").ok_or("TARSSH_DEPLOYMENT")?;
        if deployment.is_empty()
            || deployment.len() > 64
            || !deployment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
        {
            return Err("TARSSH_DEPLOYMENT");
        }
        let raw_url = get("TARSSH_ALLOY_URL").ok_or("TARSSH_ALLOY_URL")?;
        let alloy = reqwest::Url::parse(&raw_url).map_err(|_| "TARSSH_ALLOY_URL")?;
        // Reject even empty userinfo, which URL normalization can otherwise hide.
        let authority = raw_url
            .split_once("://")
            .map(|(_, rest)| rest.split(['/', '?', '#']).next().unwrap_or(""));
        if !matches!(alloy.scheme(), "http" | "https")
            || alloy.host_str().is_none()
            || !alloy.username().is_empty()
            || alloy.password().is_some()
            || alloy.query().is_some()
            || alloy.fragment().is_some()
            || authority.is_some_and(|a| a.contains('@'))
        {
            return Err("TARSSH_ALLOY_URL");
        }
        Ok(Self {
            listen: address("TARSSH_LISTEN_ADDR", "0.0.0.0:2222")?,
            metrics: address("TARSSH_METRICS_ADDR", "0.0.0.0:9000")?,
            delay: Duration::from_millis(number("TARSSH_DELAY_MS", 10000, 1, 86400000)?),
            max_line: number("TARSSH_MAX_LINE_LENGTH", 32, 3, 255)? as usize,
            max_connections: number("TARSSH_MAX_CONNECTIONS", 4096, 1, 65536)? as usize,
            write_timeout: Duration::from_millis(number(
                "TARSSH_WRITE_TIMEOUT_MS",
                5000,
                1,
                60000,
            )?),
            alloy,
            deployment,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parse(extra: &[(&str, &str)]) -> Result<Config, &'static str> {
        Config::parse(|key| {
            extra
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
                .or_else(|| match key {
                    "TARSSH_ALLOY_URL" => Some("http://127.0.0.1:9999/loki/api/v1/push".into()),
                    "TARSSH_DEPLOYMENT" => Some("edge-1".into()),
                    _ => None,
                })
        })
    }
    #[test]
    fn defaults_and_valid_boundaries() {
        let c = parse(&[]).unwrap();
        assert_eq!(c.listen, "0.0.0.0:2222".parse().unwrap());
        assert_eq!(c.metrics, "0.0.0.0:9000".parse().unwrap());
        assert_eq!(c.delay, Duration::from_secs(10));
        assert_eq!(c.write_timeout, Duration::from_secs(5));
        assert_eq!((c.max_line, c.max_connections), (32, 4096));
        assert_eq!(c.deployment, "edge-1");
        assert_eq!(c.alloy.path(), "/loki/api/v1/push");
        assert!(
            parse(&[
                ("TARSSH_LISTEN_ADDR", "[::1]:1234"),
                ("TARSSH_MAX_LINE_LENGTH", "255"),
                ("TARSSH_MAX_CONNECTIONS", "65536"),
                ("TARSSH_DELAY_MS", "86400000"),
                ("TARSSH_WRITE_TIMEOUT_MS", "60000")
            ])
            .is_ok()
        );
    }
    #[test]
    fn rejects_invalid_bounded_config_without_echoing_secrets() {
        for (key, values) in [
            (
                "TARSSH_ALLOY_URL",
                vec![
                    "",
                    "file:///secret",
                    "http://user:secret@localhost/push",
                    "http://localhost/push?secret",
                    "http://localhost/push#secret",
                    "garbage",
                ],
            ),
            ("TARSSH_DEPLOYMENT", vec!["", "a b", "x\n", "é"]),
            (
                "TARSSH_MAX_LINE_LENGTH",
                vec!["0", "2", "256", "9999999999999999999999"],
            ),
            ("TARSSH_MAX_CONNECTIONS", vec!["0", "65537"]),
            ("TARSSH_DELAY_MS", vec!["0", "86400001", "-1", "no"]),
            ("TARSSH_WRITE_TIMEOUT_MS", vec!["0", "60001"]),
            (
                "TARSSH_LISTEN_ADDR",
                vec!["localhost:2222", "127.0.0.1:0", "127.0.0.1:65536"],
            ),
            ("TARSSH_METRICS_ADDR", vec!["127.0.0.1:0"]),
        ] {
            for value in values {
                let error = parse(&[(key, value)]).unwrap_err();
                assert!(!error.contains("secret"));
                assert!(!error.contains(value) || value.is_empty());
            }
        }
        assert!(parse(&[("TARSSH_DEPLOYMENT", &"a".repeat(65))]).is_err());
        assert!(Config::parse(|_| None).is_err());
    }
}
