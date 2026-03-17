# Python ETL — Pipeline Orchestrator

> Production-grade Python pipeline orchestrator for loading healthcare data into Snowflake. Modular, observable, and safe to run in any scheduler (Airflow, cron, GitHub Actions).

---

## Structure

```
python_etl/
├── pipeline_orchestrator.py   # Main orchestrator
└── requirements.txt           # Dependencies
```

---

## Architecture

The orchestrator is built around three core abstractions:

### `PipelineStep`
A single unit of work. Subclass it and implement `execute()`. The base class handles timing, logging, and error capture automatically.

Built-in steps:
| Step | What It Does |
|---|---|
| `StageLoadStep` | COPY INTO from S3 stage → Snowflake landing table |
| `TransformStep` | Calls a stored procedure (SCD2 merge, fact load, etc.) |
| `DQCheckStep` | Runs the full DQ suite; halts pipeline if any FAIL found |
| `AuditLogStep` | Writes run summary to `governance.pipeline_runs` |

### `Pipeline`
Runs a list of steps in order. Fail-fast — halts on first failure. Always writes the audit log, even when it fails.

### `PipelineRun`
A dataclass that tracks the run's `run_id`, status, step results, total rows, and timing. Serialises to JSON for the audit table.

---

## Key Features

**Structured JSON logging** — every log line is a JSON object. Plug directly into Datadog, CloudWatch, or Splunk with zero extra config.

**Retry with exponential backoff** — the `@retry` decorator wraps any function. Snowflake connection attempts retry 3 times (2s → 4s → 8s).

**DQ gate** — `DQCheckStep` calls the Snowflake DQ framework and raises an exception if any check returns `FAIL`. The pipeline halts before bad data reaches downstream.

**Fail-fast + always audit** — even on failure, the run metadata is written to `governance.pipeline_runs` so you always have a full audit trail.

**Alerting hook** — `_send_alert()` logs a CRITICAL structured message on failure. Wire this to Slack, PagerDuty, or SNS by adding a few lines.

---

## Environment Variables

Never hardcode credentials. Set these before running:

```bash
export SNOWFLAKE_ACCOUNT=myorg-myaccount
export SNOWFLAKE_USER=etl_service_user
export SNOWFLAKE_PASSWORD=***
export SNOWFLAKE_WAREHOUSE=LOAD_WH
export SNOWFLAKE_DATABASE=healthcare_dw
export SNOWFLAKE_SCHEMA=raw
export SNOWFLAKE_ROLE=ETL_ROLE
export ALERT_SLACK_CHANNEL=#data-alerts
```

---

## Running Pipelines

```bash
# Install dependencies
pip install -r requirements.txt

# Run the daily encounters pipeline
python pipeline_orchestrator.py encounters

# Run the incremental claims pipeline
python pipeline_orchestrator.py claims
```

Exit code `0` = success, `1` = failure — compatible with any scheduler.

---

## Adding a New Pipeline

```python
def build_my_pipeline() -> Pipeline:
    return Pipeline(
        name  = "my_new_pipeline",
        steps = [
            StageLoadStep("raw.my_stage", "raw.my_table"),
            TransformStep("raw.sp_my_transform", "transform_my_data"),
            DQCheckStep(),
        ]
    )
```

That's it. The orchestrator handles logging, retries, DQ gating, and audit writing for you.
