use anyhow::{Context, Result};
use aws_sdk_cloudwatchlogs::Client;
use tokio::time::{sleep, Duration};

#[derive(Clone)]
pub struct CwCounter {
    client: Client,
    log_group: String,
}

impl CwCounter {
    pub fn new(client: Client, log_group: String) -> Self {
        Self { client, log_group }
    }

    pub async fn count_range(&self, start_ms: i64, end_ms: i64) -> Result<u64> {
        let query = "stats count(*)";
        let start = start_ms / 1000;
        let end = end_ms / 1000;

        let start_resp = self
            .client
            .start_query()
            .log_group_name(&self.log_group)
            .start_time(start)
            .end_time(end)
            .query_string(query)
            .limit(1)
            .send()
            .await
            .context("cw start_query")?;

        let qid = start_resp
            .query_id()
            .ok_or_else(|| anyhow::anyhow!("missing query_id"))?
            .to_string();

        // Poll results
        for _ in 0..30 {
            let res = self
                .client
                .get_query_results()
                .query_id(&qid)
                .send()
                .await
                .context("cw get_query_results")?;

            if let Some(status) = res.status() {
                match status.as_str() {
                    "Complete" => {
                        let results = res.results();
                        if let Some(row) = results.first() {
                            for field in row {
                                if let (Some(k), Some(v)) = (field.field(), field.value()) {
                                    if k == "count" {
                                        if let Ok(n) = v.parse::<u64>() {
                                            return Ok(n);
                                        }
                                    }
                                }
                            }
                        }
                        return Ok(0);
                    }
                    "Failed" | "Cancelled" | "Timeout" => {
                        anyhow::bail!("cw query failed status={}", status.as_str());
                    }
                    _ => {}
                }
            }
            sleep(Duration::from_millis(500)).await;
        }

        anyhow::bail!("cw query timed out");
    }
}
