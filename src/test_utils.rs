#[cfg(test)]
use crate::{client::Pulsar, executor::TokioExecutor};

/// Wrapper for the Tokio executor
#[cfg(any(
    feature = "tokio-runtime",
    feature = "tokio-rustls-runtime-aws-lc-rs",
    feature = "tokio-rustls-runtime-ring"
))]
#[cfg(test)]
pub async fn new_pulsar() -> Pulsar<TokioExecutor> {
    use log::LevelFilter;

    use crate::tests::TEST_LOGGER;

    let _result = log::set_logger(&TEST_LOGGER);
    log::set_max_level(LevelFilter::Debug);

    Pulsar::builder("pulsar://127.0.0.1:6650", TokioExecutor)
        .build()
        .await
        .unwrap()
}

#[cfg(test)]
pub(crate) async fn create_partitioned_topic(
    tenant: &str,
    namespace: &str,
    topic_name: &str,
    num_partitions: u32,
) {
    use reqwest::Client;

    let create_partitioned_topic_url = format!(
        "http://127.0.0.1:8080/admin/v2/persistent/{tenant}/{namespace}/{topic_name}/partitions"
    );
    let client = Client::new();
    let response = client
        .put(create_partitioned_topic_url)
        .json(&num_partitions.to_string())
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
}

/// Unloads a topic, which makes the broker close its producers and consumers.
#[cfg(test)]
pub(crate) async fn unload_topic(tenant: &str, namespace: &str, topic_name: &str) {
    let url = format!(
        "http://127.0.0.1:8080/admin/v2/persistent/{tenant}/{namespace}/{topic_name}/unload"
    );
    let response = reqwest::Client::new().put(url).send().await.unwrap();
    assert!(response.status().is_success());
}

/// The admin API's stats for the first consumer on `subscription` of
/// `persistent://public/default/{topic_name}`, once `ready` holds for them.
/// The broker has no event for its counters, so this polls them (every
/// 50 ms, failing after 30 s); what a test asserts never depends on timing.
#[cfg(test)]
pub(crate) async fn consumer_stats_when(
    topic_name: &str,
    subscription: &str,
    ready: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let url =
        format!("http://127.0.0.1:8080/admin/v2/persistent/public/default/{topic_name}/stats");
    let client = reqwest::Client::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut last = serde_json::Value::Null;
    while std::time::Instant::now() < deadline {
        // a stats call that fails (the topic is moving, e.g. after an
        // unload) counts as not ready yet
        let stats: serde_json::Value = match client.get(&url).send().await {
            Ok(resp) => resp.json().await.unwrap_or_default(),
            Err(_) => serde_json::Value::Null,
        };
        last = stats["subscriptions"][subscription]["consumers"][0].clone();
        if !last.is_null() && ready(&last) {
            return last;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("consumer stats never became ready: {last}");
}

/// The test broker's version as the admin API reports it: some
/// consume-path tests assert per measured broker version (a broker's
/// dispatcher changed how it treats a forged batch count between releases).
#[cfg(test)]
pub(crate) async fn broker_version() -> String {
    reqwest::get("http://127.0.0.1:8080/admin/v2/brokers/version")
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

/// A plain `major.minor.patch` release version, `None` for anything else
/// (a SNAPSHOT or other suffix, a fourth part, garbage): such a broker is
/// unmeasured.
#[cfg(test)]
pub(crate) fn release_version(text: &str) -> Option<(u32, u32, u32)> {
    let mut parts = text.trim().trim_matches('"').split('.');
    let mut next = || {
        parts
            .next()
            .filter(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|p| p.parse().ok())
    };
    let version = (next()?, next()?, next()?);
    parts.next().is_none().then_some(version)
}

#[cfg(test)]
mod release_version_tests {
    use super::release_version;

    #[test]
    fn only_a_plain_release_version_parses() {
        assert_eq!(release_version("4.0.8"), Some((4, 0, 8)));
        assert_eq!(release_version("\"5.0.0\"\n"), Some((5, 0, 0)));
        assert_eq!(release_version("4.0.8-SNAPSHOT"), None);
        assert_eq!(release_version("4.0.8.1"), None);
        assert_eq!(release_version("4.0"), None);
        assert_eq!(release_version("v4.0.8"), None);
        assert_eq!(release_version(""), None);
    }
}
