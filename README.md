# logstream

**High-performance CloudWatch Logs streaming to Elasticsearch**

A production-ready Rust service that continuously streams AWS CloudWatch Logs to Elasticsearch with guaranteed data integrity, automatic conflict resolution, and intelligent adaptive performance tuning.

## Why logstream?

### vs Logstash + Filebeat

- **10x lower resource usage** - Single binary, minimal memory footprint
- **Zero data loss** - Almost-sure reconciliation with mathematical guarantees
- **Self-healing** - Automatic schema drift detection and resolution
- **Simpler deployment** - No JVM, no plugins, no complex pipelines

### vs AWS Lambda + Kinesis

- **Lower cost** - No per-invocation charges or Kinesis streams
- **Complete history** - Built-in backfill for years of logs
- **Better reliability** - No Lambda cold starts or timeout issues
- **Data integrity** - Automatic verification and reconciliation

### vs CloudWatch Logs Insights

- **Full-text search** - Elasticsearch's powerful query DSL
- **Cost effective** - No per-query charges
- **Long retention** - Keep logs as long as needed
- **Custom visualization** - Kibana dashboards and alerts

## Key Features

### Data Integrity

**Almost-Sure Reconciliation**

- Uses O(log² n) sampling algorithms to verify data completeness
- Detects missing events across time ranges with probabilistic guarantees
- Automatic gap filling with priority-based backfill
- No silent data loss - every discrepancy is detected and resolved

**Seasonal Anomaly Detection**

- Learns normal log volume patterns (hourly, daily, weekly, monthly cycles)
- Detects suspicious zero-count periods that indicate missing data
- Adapts to traffic changes and seasonal variations
- Prevents false positives during legitimate downtime

**Schema Healing**

- Detects type conflicts across indices (e.g., field is `string` in one index, `long` in another)
- Consensus-based resolution - majority type wins, outliers are rebuilt
- Automatic index cleanup and reingestion
- Fixes Kibana "mapping conflict" warnings automatically

### Performance & Reliability

**Adaptive Throughput Control**

- Real-time adjustment of batch size and concurrency based on latency
- Automatic backpressure handling when ES or CloudWatch are stressed
- Converges to optimal throughput without manual tuning
- CPU and memory pressure monitoring with emergency throttling

**Multi-Level Priority System**

- **CRITICAL** - Real-time tail (last hour)
- **HIGH** - Recent data (last week) and healing
- **NORMAL** - Reconciliation (last month)
- **LOW** - Historical backfill (2+ months)
- **IDLE** - Deep history (1+ year)

Ensures real-time logs are never blocked by background operations.

**Stress Management**

- Exponential backoff for CloudWatch throttling
- ES bulk failure classification (transient vs permanent)
- Automatic retry with circuit breaker patterns
- Rate limiting based on observed error rates

### Multi-Group Support

**Concurrent Processing**

- Stream multiple log groups simultaneously
- Independent checkpoints per group
- Per-group data streams with stable names (see **Data stream organization** below)
- Unified querying via `cloudwatch-*` (or your configured `INDEX_PREFIX-*`)

**Data stream organization**

```
cloudwatch-ecs-app-prod-service           # Stable name (alias)
cloudwatch-ecs-app-prod-service-v1        # Versioned data stream (active)
cloudwatch-ecs-app-prod-service-v2        # Versioned data stream (standby/repair)
.ds-cloudwatch-ecs-app-prod-service-v1-2025.12.21-000001  # Backing index (managed by ES)
```

### Checkpointing & Recovery

**Persistent State**

- File-based checkpoints with atomic updates
- Per-stream cursor tracking
- Survives restarts without data loss or duplication
- Configurable checkpoint location

**Conflict Resolution**

- Detects overlapping or duplicate events
- Deduplication based on CloudWatch event IDs
- Handles out-of-order delivery
- Resolves checkpoint corruption

### Data Processing

**JSON Parsing & Enrichment**

- Automatic JSON detection and parsing
- Nested object flattening for ES compatibility
- Field sanitization (removes invalid characters)
- Type coercion for problematic values (NaN, Infinity → null)

**Gzip Compression**

- Automatic compression for bulk requests
- Reduces network bandwidth by ~80%
- Configurable batch sizes

### Operational Features

**Backfill Support**

- Stream years of historical data efficiently
- Configurable concurrency and batch tuning
- Priority-based scheduling (recent data first)
- Progress tracking and ETA

**Schema Healing**

- Detects mapping conflicts across indices
- Samples actual data to verify types
- Deletes and rebuilds problematic indices
- Runs automatically during reconciliation

**Health Monitoring**

- Detailed logging with configurable levels
- Throughput metrics (events/sec)
- Latency tracking for adaptive control
- Stress level indicators

## Quick Start

### Docker (Recommended)

```bash
docker pull ghcr.io/dbpiper/logstream:latest

docker run --rm \
  -e LOG_GROUPS=/aws/lambda/my-function,/ecs/my-service \
  -e AWS_REGION=us-east-1 \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  -e ES_HOST=http://elasticsearch:9200 \
  -e ES_USER=elastic \
  -e ES_PASS=changeme \
  -v $(pwd)/state:/state \
  ghcr.io/dbpiper/logstream:latest
```

### From Source

```bash
cargo build --release
./target/release/logstream
```

## Configuration

Configuration via environment variables or `config.toml`:

### Required

| Variable     | Description                                   | Default      |
| ------------ | --------------------------------------------- | ------------ |
| `LOG_GROUPS` | Comma-separated list of CloudWatch log groups | **required** |
| `AWS_REGION` | AWS region                                    | **required** |

### Elasticsearch

| Variable       | Description                | Default                 |
| -------------- | -------------------------- | ----------------------- |
| `ES_HOST`      | Elasticsearch URL          | `http://localhost:9200` |
| `ES_USER`      | Elasticsearch username     | `elastic`               |
| `ES_PASS`      | Elasticsearch password     | `changeme`              |
| `INDEX_PREFIX` | Elasticsearch index prefix | `cloudwatch`            |

**Note:** `INDEX_PREFIX` defaults to `cloudwatch` to avoid conflicts with ES built-in `logs-*` data stream templates and data views.

### Performance Tuning

| Variable             | Description              | Default |
| -------------------- | ------------------------ | ------- |
| `BATCH_SIZE`         | Events per bulk request  | `100`   |
| `MAX_IN_FLIGHT`      | Concurrent bulk requests | `2`     |
| `POLL_INTERVAL_SECS` | Tail poll interval       | `15`    |
| `HTTP_TIMEOUT_SECS`  | HTTP request timeout     | `30`    |
| `BACKOFF_BASE_MS`    | Initial backoff delay    | `200`   |
| `BACKOFF_MAX_MS`     | Maximum backoff delay    | `10000` |

### Reconciliation & Backfill

| Variable                  | Description                                    | Default                   |
| ------------------------- | ---------------------------------------------- | ------------------------- |
| `RECONCILE_INTERVAL_SECS` | How often to verify data completeness          | `900` (15 min)            |
| `BACKFILL_DAYS`           | Days of historical data to stream on first run | `30`                      |
| `CHECKPOINT_PATH`         | Where to store checkpoint state                | `/state/checkpoints.json` |

### Discover visibility (refresh policy)

logstream manages backing index refresh so Kibana Discover stays live while keeping overhead reasonable:

- Backing indices whose `max(@timestamp)` is within the last **7 days**: `refresh_interval=1s`
- Older backing indices: `refresh_interval=30s`

You can tune this behavior with:

| Variable                        | Description                                             | Default |
| ------------------------------- | ------------------------------------------------------- | ------- |
| `ES_HOT_REFRESH_INTERVAL`       | Refresh interval for “hot” backing indices              | `1s`    |
| `ES_COLD_REFRESH_INTERVAL`      | Refresh interval for “cold” backing indices             | `30s`   |
| `ES_COLD_AGE_DAYS`              | Backing index is “cold” if max `@timestamp` is older    | `7`     |
| `ES_REFRESH_TUNE_INTERVAL_SECS` | How often to re-evaluate backing index refresh settings | `3600`  |

### Advanced

**Adaptive controller** automatically adjusts `BATCH_SIZE` and `MAX_IN_FLIGHT` based on:

- ES bulk request latency
- CloudWatch API throttling
- CPU and memory pressure
- Error rates

**Stress tracking** implements exponential backoff when:

- CloudWatch returns throttling errors
- ES returns 429 (too many requests)
- Bulk requests fail repeatedly

## Index / data stream structure

logstream writes to **data streams** (not daily indices). Each log group has a stable name and two versioned data streams to support safe repairs.

```
{INDEX_PREFIX}-{sanitized-group-name}         # Stable name (alias)
{INDEX_PREFIX}-{sanitized-group-name}-v1      # Versioned data stream
{INDEX_PREFIX}-{sanitized-group-name}-v2      # Versioned data stream
```

**Example:**

```
cloudwatch-ecs-myapp-prod-service
cloudwatch-ecs-myapp-prod-service-v1
cloudwatch-ecs-myapp-prod-service-v2
```

**Cumulative view:**

```
cloudwatch-*  # Query all logstream streams (or {INDEX_PREFIX}-*)
```

### Per-Group Data Streams

Each log group gets its own index pattern, allowing you to:

- Set different retention policies per group
- Query specific services independently
- Maintain isolated schemas
- Scale storage per service

## Reconciliation Algorithm

logstream uses **almost-sure reconciliation** - a probabilistic algorithm with mathematical guarantees:

1. **Sampling**: Select random time ranges using O(log² n) algorithm
2. **Counting**: Compare CloudWatch vs Elasticsearch event counts
3. **Verification**: Check data integrity (timestamp distribution, gaps)
4. **Learning**: Build seasonal models of normal log volume
5. **Resolution**: Fill gaps with appropriate priority

**Seasonal Learning:**

- Tracks patterns at multiple scales (hour, day, week, month)
- Uses kernel density estimation for smooth predictions
- Adapts to regime changes (deployments, traffic shifts)
- Prevents false alarms during legitimate zero-traffic periods

**Feasibility Check:**

```rust
// Example: Is 0 events plausible at 3am on Sunday?
if seasonal_model.is_feasible(0, timestamp, stress_level) {
    // Normal - many services are quiet at night
    trust_es();
} else {
    // Suspicious - investigate and backfill
    trigger_reconciliation();
}
```

## Schema Healing

Detects and fixes two types of conflicts:

### 1. Data vs Mapping Conflicts

When data type doesn't match ES mapping (e.g., ES expects object but gets string)

### 2. Cross-Index Mapping Conflicts

When the same field has different types across indices:

```
cloudwatch-2025.12.10: { "statusCode": "200" }  // string
cloudwatch-2025.12.11: { "statusCode": 200 }    // long
```

**Resolution:**

1. Fetch mappings from all indices in the pattern
2. Build consensus (majority type wins)
3. Delete minority indices (those using less common type)
4. Reconciliation rebuilds deleted indices with correct type

**Result:** Kibana "mapping conflict" warnings disappear.

## Priority System

Tasks are assigned priorities that determine execution order:

| Priority     | Use Case                   | Preemption                   |
| ------------ | -------------------------- | ---------------------------- |
| **CRITICAL** | Real-time tail (last hour) | Never paused                 |
| **HIGH**     | Recent backfill, healing   | Only on critical stress      |
| **NORMAL**   | Reconciliation             | Pauses under moderate stress |
| **LOW**      | Historical backfill        | Pauses under any stress      |
| **IDLE**     | Deep history (1+ year)     | Most aggressive pausing      |

This ensures recent logs are always delivered, even when the system is catching up on years of history.

## Monitoring

logstream emits structured logs with metrics:

```
INFO logstream: tail: fetched 1234 events for /aws/lambda/api
INFO logstream::es_bulk_sink: es bulk: ingested 1234 docs in 45ms
INFO logstream::adaptive: adaptive: batch=200 in_flight=3 latency_p50=23ms
INFO logstream::reconcile: reconcile: day 2025-12-14 complete, es=125000 cw=125000 match=true
INFO logstream::es_schema_heal: schema_heal: fixed 2 indices with mapping conflicts
```

## Troubleshooting

### No logs appearing in Elasticsearch

1. Check ES is reachable: `curl $ES_HOST/_cluster/health`
2. Verify credentials work
3. Check logstream logs for errors
4. Verify log group exists in CloudWatch
5. In Kibana Discover, ensure your data view matches `cloudwatch-*` (not `logs-*`) and the time field is `@timestamp`

### Missing historical data

- Increase `BACKFILL_DAYS` to stream more history
- Check reconciliation logs for gap detection
- Verify CloudWatch retention period

### High memory usage

- Reduce `BATCH_SIZE` and `MAX_IN_FLIGHT`
- Adaptive controller may be too aggressive
- Check for large individual log messages

### ES bulk failures

- Check ES cluster health (CPU, memory, disk)
- Increase `HTTP_TIMEOUT_SECS`
- Reduce concurrency with `MAX_IN_FLIGHT`
- Check ES logs for rejection reasons

## Architecture

```
┌─────────────────┐
│  CloudWatch     │
│  Log Groups     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌──────────────┐
│   Tailer        │────▶│ Checkpoint   │
│   (per group)   │     │   State      │
└────────┬────────┘     └──────────────┘
         │
         ▼
┌─────────────────┐
│  Event Router   │ ◀──── Priority-based scheduling
│  (5 priorities) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Bulk Sink      │ ◀──── Adaptive controller
│  (ES ingest)    │       Stress tracker
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Elasticsearch   │
│   Indices       │
└─────────────────┘

Background:
┌─────────────────┐
│ Reconciliation  │ ◀──── Seasonal stats
│   Daemon        │       Schema healer
└─────────────────┘
```

## License

MIT

## Contributing

Contributions welcome! Please ensure:

- Tests pass: `make tools && make test` (uses project-local `headlamp`)
- Fast local loop: `make tools && make test-dev`
- No clippy warnings: `cargo clippy -- -D warnings`
- Code is formatted: `cargo fmt`
