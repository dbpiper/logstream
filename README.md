# logstream

Continuous CloudWatch Logs tailer in Rust that ships NDJSON batches directly to Elasticsearch with checkpoints, retry/backoff, and almost-sure reconciliation.

A drop-in replacement for Logstash + Filebeat with lower resource usage and simpler deployment.

## Features

- Real-time CloudWatch Logs tailing with checkpointing
- Bulk ingestion to Elasticsearch with gzip compression
- Almost-sure reconciliation using O(log² n) sampling algorithms
- Schema drift detection and automatic healing
- Backfill support for historical data
- Configurable batching, concurrency, and backoff

## Quick Start

### Docker (recommended)

```bash
docker pull ghcr.io/dbpiper/logstream:latest

docker run --rm \
  -e LOG_GROUPS=/aws/lambda/my-function \
  -e AWS_REGION=us-east-1 \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  -e ES_HOST=http://elasticsearch:9200 \
  -e ES_USER=elastic \
  -e ES_PASS=changeme \
  -v $(pwd)/state:/state \
  ghcr.io/dbpiper/logstream:latest
```

### From source

```bash
cargo build --release
./target/release/logstream ./config.toml
```

## Configuration

Configuration via `config.toml` or environment variables:

| Variable                  | Description                        | Default                   |
| ------------------------- | ---------------------------------- | ------------------------- |
| `LOG_GROUPS`              | Comma-separated list of log groups | required                  |
| `AWS_REGION`              | AWS region                         | required                  |
| `ES_HOST`                 | Elasticsearch URL                  | `http://localhost:9200`   |
| `ES_USER`                 | Elasticsearch username             | `elastic`                 |
| `ES_PASS`                 | Elasticsearch password             | `changeme`                |
| `INDEX_PREFIX`            | Elasticsearch index prefix         | `logs`                    |
| `BATCH_SIZE`              | Events per bulk request            | `100`                     |
| `MAX_IN_FLIGHT`           | Concurrent bulk requests           | `2`                       |
| `POLL_INTERVAL_SECS`      | Tail poll interval                 | `15`                      |
| `RECONCILE_INTERVAL_SECS` | Reconciliation interval            | `900`                     |
| `BACKFILL_DAYS`           | Days of history to backfill        | `30`                      |
| `CHECKPOINT_PATH`         | Checkpoint file path               | `/state/checkpoints.json` |
| `HTTP_TIMEOUT_SECS`       | HTTP request timeout               | `30`                      |
| `BACKOFF_BASE_MS`         | Base backoff delay                 | `200`                     |
| `BACKOFF_MAX_MS`          | Max backoff delay                  | `10000`                   |

`INDEX_PREFIX` defaults to `logs`. Indices are per log group per day (`${INDEX_PREFIX}-{group}-{YYYY.MM.DD}`) and a lightweight alias `${INDEX_PREFIX}-all` points to `${INDEX_PREFIX}-*` for the cumulative data view without duplicating data.

## License

MIT
