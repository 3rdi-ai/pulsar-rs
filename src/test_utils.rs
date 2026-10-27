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
        let stats: serde_json::Value = client.get(&url).send().await.unwrap().json().await.unwrap();
        last = stats["subscriptions"][subscription]["consumers"][0].clone();
        if !last.is_null() && ready(&last) {
            return last;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("consumer stats never became ready: {last}");
}
