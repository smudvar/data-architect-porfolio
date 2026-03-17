"""
pipeline_orchestrator.py
========================
Production-grade ETL pipeline orchestrator for loading healthcare
data into Snowflake. Features:
  - Modular, step-based pipeline definition
  - Structured JSON logging at every step
  - Retry logic with exponential backoff
  - Run metadata written to a pipeline_runs audit table
  - Slack/email alerting on failure
  - CLI interface for manual or scheduled runs
"""

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from functools import wraps
from typing import Any, Callable, Dict, List, Optional

import snowflake.connector
from snowflake.connector import DictCursor


# ── Logging setup — structured JSON to stdout (Datadog/CloudWatch friendly) ──
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_obj = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level":     record.levelname,
            "logger":    record.name,
            "message":   record.getMessage(),
            "module":    record.module,
            "line":      record.lineno,
        }
        if hasattr(record, "extra"):
            log_obj.update(record.extra)
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_obj)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


logger = get_logger("pipeline_orchestrator")


# ── Enums ─────────────────────────────────────────────────────────────────────
class StepStatus(str, Enum):
    PENDING  = "pending"
    RUNNING  = "running"
    SUCCESS  = "success"
    FAILED   = "failed"
    SKIPPED  = "skipped"


class PipelineStatus(str, Enum):
    RUNNING  = "running"
    SUCCESS  = "success"
    FAILED   = "failed"


# ── Data classes ──────────────────────────────────────────────────────────────
@dataclass
class StepResult:
    step_name:      str
    status:         StepStatus
    rows_affected:  int          = 0
    duration_sec:   float        = 0.0
    error_message:  Optional[str] = None
    metadata:       Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineRun:
    run_id:         str          = field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_name:  str          = ""
    status:         PipelineStatus = PipelineStatus.RUNNING
    started_at:     datetime     = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at:    Optional[datetime] = None
    step_results:   List[StepResult]   = field(default_factory=list)
    total_rows:     int          = 0
    error_message:  Optional[str] = None

    @property
    def duration_sec(self) -> float:
        if self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return (datetime.now(timezone.utc) - self.started_at).total_seconds()

    def to_dict(self) -> Dict:
        d = asdict(self)
        d["started_at"]  = self.started_at.isoformat()
        d["finished_at"] = self.finished_at.isoformat() if self.finished_at else None
        d["duration_sec"] = self.duration_sec
        return d


# ── Retry decorator ───────────────────────────────────────────────────────────
def retry(max_attempts: int = 3, backoff_base: float = 2.0, exceptions=(Exception,)):
    """
    Retry a function up to max_attempts times with exponential backoff.
    Example: @retry(max_attempts=3, backoff_base=2.0)
    Waits: 2s, 4s, 8s between attempts.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    wait = backoff_base ** attempt
                    logger.warning(
                        f"Attempt {attempt}/{max_attempts} failed for "
                        f"'{func.__name__}': {exc}. Retrying in {wait}s..."
                    )
                    if attempt < max_attempts:
                        time.sleep(wait)
            raise last_exc
        return wrapper
    return decorator


# ── Snowflake connection ──────────────────────────────────────────────────────
class SnowflakeConnection:
    """
    Context manager that yields a Snowflake connection.
    Reads credentials from environment variables — never hardcoded.

    Required env vars:
        SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD (or key-pair),
        SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
    """

    def __init__(self, role: Optional[str] = None):
        self.role = role
        self._conn = None

    @retry(max_attempts=3, backoff_base=2.0,
           exceptions=(snowflake.connector.errors.DatabaseError,))
    def _connect(self):
        return snowflake.connector.connect(
            account   = os.environ["SNOWFLAKE_ACCOUNT"],
            user      = os.environ["SNOWFLAKE_USER"],
            password  = os.environ["SNOWFLAKE_PASSWORD"],
            warehouse = os.environ["SNOWFLAKE_WAREHOUSE"],
            database  = os.environ["SNOWFLAKE_DATABASE"],
            schema    = os.environ["SNOWFLAKE_SCHEMA"],
            role      = self.role or os.environ.get("SNOWFLAKE_ROLE"),
            session_parameters={"QUERY_TAG": "pipeline_orchestrator"},
        )

    def __enter__(self):
        self._conn = self._connect()
        logger.info("Snowflake connection established.")
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn:
            self._conn.close()
            logger.info("Snowflake connection closed.")
        return False   # don't suppress exceptions


# ── Individual pipeline steps ─────────────────────────────────────────────────
class PipelineStep:
    """
    Base class for a pipeline step. Subclass and implement `execute()`.
    Handles timing, logging, and error capture automatically.
    """

    name: str = "base_step"

    def execute(self, conn, run: PipelineRun) -> StepResult:
        raise NotImplementedError

    def run(self, conn, pipeline_run: PipelineRun) -> StepResult:
        logger.info(f"Starting step: {self.name}",
                    extra={"run_id": pipeline_run.run_id, "step": self.name})
        start = time.monotonic()
        try:
            result = self.execute(conn, pipeline_run)
            result.duration_sec = time.monotonic() - start
            result.status = StepStatus.SUCCESS
            logger.info(
                f"Step '{self.name}' succeeded — "
                f"{result.rows_affected} rows in {result.duration_sec:.2f}s",
                extra={"run_id": pipeline_run.run_id, "step": self.name,
                       "rows": result.rows_affected}
            )
        except Exception as exc:
            duration = time.monotonic() - start
            result = StepResult(
                step_name     = self.name,
                status        = StepStatus.FAILED,
                duration_sec  = duration,
                error_message = str(exc),
            )
            logger.error(
                f"Step '{self.name}' FAILED after {duration:.2f}s: {exc}",
                extra={"run_id": pipeline_run.run_id, "step": self.name},
                exc_info=True,
            )
        return result


class StageLoadStep(PipelineStep):
    """Copy raw CSV/JSON files from S3 into the Snowflake staging table."""
    name = "stage_load"

    def __init__(self, stage_name: str, target_table: str, file_pattern: str = "*"):
        self.stage_name   = stage_name
        self.target_table = target_table
        self.file_pattern = file_pattern

    def execute(self, conn, run: PipelineRun) -> StepResult:
        sql = f"""
            COPY INTO {self.target_table}
            FROM   @{self.stage_name}/{self.file_pattern}
            FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
            ON_ERROR    = 'CONTINUE'
            PURGE       = FALSE;
        """
        with conn.cursor(DictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            rows_loaded = sum(r.get("rows_loaded", 0) for r in rows)
            return StepResult(
                step_name    = self.name,
                status       = StepStatus.RUNNING,
                rows_affected= rows_loaded,
                metadata     = {"copy_results": rows},
            )


class TransformStep(PipelineStep):
    """Execute a stored procedure that transforms staged data into the fact layer."""
    name = "transform"

    def __init__(self, procedure_name: str, step_label: str = None):
        self.procedure_name = procedure_name
        self.name = step_label or f"transform_{procedure_name}"

    def execute(self, conn, run: PipelineRun) -> StepResult:
        with conn.cursor() as cur:
            cur.execute(f"CALL {self.procedure_name}('{run.run_id}');")
            result_msg = cur.fetchone()[0]
            return StepResult(
                step_name = self.name,
                status    = StepStatus.RUNNING,
                metadata  = {"procedure_result": result_msg},
            )


class DQCheckStep(PipelineStep):
    """Run the data quality suite and fail the pipeline if any FAIL checks exist."""
    name = "dq_checks"

    def execute(self, conn, run: PipelineRun) -> StepResult:
        with conn.cursor(DictCursor) as cur:
            # Execute all DQ checks
            cur.execute(f"CALL governance.sp_run_all_dq_checks();")

            # Count failures in this run
            cur.execute("""
                SELECT status, COUNT(*) AS cnt
                FROM   governance.dq_results
                WHERE  run_id = (
                    SELECT run_id FROM governance.dq_results
                    ORDER BY checked_at DESC LIMIT 1
                )
                GROUP BY status;
            """)
            counts = {row["STATUS"]: row["CNT"] for row in cur.fetchall()}

        fail_count = counts.get("FAIL", 0)
        warn_count = counts.get("WARN", 0)
        pass_count = counts.get("PASS", 0)

        if fail_count > 0:
            raise ValueError(
                f"DQ suite found {fail_count} FAIL(s), {warn_count} WARN(s). "
                f"Pipeline halted. Check governance.v_dq_latest_status."
            )

        return StepResult(
            step_name    = self.name,
            status       = StepStatus.RUNNING,
            metadata     = {"pass": pass_count, "warn": warn_count, "fail": fail_count},
        )


class AuditLogStep(PipelineStep):
    """Write the final pipeline run summary to the audit table."""
    name = "audit_log"

    def execute(self, conn, run: PipelineRun) -> StepResult:
        summary = json.dumps(run.to_dict())
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO governance.pipeline_runs
                    (run_id, pipeline_name, status, started_at,
                     finished_at, total_rows, duration_sec, run_summary)
                SELECT
                    %s, %s, %s, %s::TIMESTAMP_NTZ,
                    %s::TIMESTAMP_NTZ, %s, %s, PARSE_JSON(%s);
            """, (
                run.run_id,
                run.pipeline_name,
                run.status.value,
                run.started_at.isoformat(),
                run.finished_at.isoformat() if run.finished_at else None,
                run.total_rows,
                run.duration_sec,
                summary,
            ))
        return StepResult(step_name=self.name, status=StepStatus.RUNNING)


# ── Pipeline orchestrator ─────────────────────────────────────────────────────
class Pipeline:
    """
    Orchestrates a sequence of PipelineStep objects.
    - Runs steps in order
    - Halts on first FAILED step (fail-fast)
    - Writes audit log regardless of outcome
    - Sends alert on failure
    """

    def __init__(self, name: str, steps: List[PipelineStep],
                 alert_on_failure: bool = True):
        self.name             = name
        self.steps            = steps
        self.alert_on_failure = alert_on_failure

    def run(self) -> PipelineRun:
        pipeline_run = PipelineRun(pipeline_name=self.name)
        logger.info(
            f"Pipeline '{self.name}' started.",
            extra={"run_id": pipeline_run.run_id, "pipeline": self.name}
        )

        with SnowflakeConnection() as conn:
            for step in self.steps:
                result = step.run(conn, pipeline_run)
                pipeline_run.step_results.append(result)
                pipeline_run.total_rows += result.rows_affected

                if result.status == StepStatus.FAILED:
                    pipeline_run.status        = PipelineStatus.FAILED
                    pipeline_run.error_message = result.error_message
                    pipeline_run.finished_at   = datetime.now(timezone.utc)
                    logger.error(
                        f"Pipeline '{self.name}' FAILED at step '{step.name}'.",
                        extra={"run_id": pipeline_run.run_id}
                    )
                    if self.alert_on_failure:
                        self._send_alert(pipeline_run)
                    # Always write audit log, even on failure
                    AuditLogStep().run(conn, pipeline_run)
                    return pipeline_run

        pipeline_run.status      = PipelineStatus.SUCCESS
        pipeline_run.finished_at = datetime.now(timezone.utc)
        logger.info(
            f"Pipeline '{self.name}' completed successfully in "
            f"{pipeline_run.duration_sec:.2f}s. Total rows: {pipeline_run.total_rows}.",
            extra={"run_id": pipeline_run.run_id}
        )

        with SnowflakeConnection() as conn:
            AuditLogStep().run(conn, pipeline_run)

        return pipeline_run

    def _send_alert(self, run: PipelineRun):
        """
        Send failure alert. Extend this to call Slack / PagerDuty / SNS.
        Currently logs a structured CRITICAL message — easy to route
        with any log aggregator (Datadog, CloudWatch, Splunk).
        """
        logger.critical(
            f"PIPELINE FAILURE ALERT: '{run.pipeline_name}'",
            extra={
                "run_id":        run.run_id,
                "pipeline":      run.pipeline_name,
                "error":         run.error_message,
                "failed_step":   next(
                    (s.step_name for s in run.step_results
                     if s.status == StepStatus.FAILED), "unknown"
                ),
                "duration_sec":  run.duration_sec,
                "alert_channel": os.environ.get("ALERT_SLACK_CHANNEL", "#data-alerts"),
            }
        )


# ── Pipeline definitions ──────────────────────────────────────────────────────
def build_encounters_pipeline() -> Pipeline:
    """Daily pipeline: load raw encounters → transform → DQ check → audit."""
    return Pipeline(
        name  = "healthcare_encounters_daily",
        steps = [
            StageLoadStep(
                stage_name   = "raw.s3_stage_encounters",
                target_table = "raw.encounters_landing",
                file_pattern = "encounters/",
            ),
            TransformStep(
                procedure_name = "raw.sp_load_fact_encounter",
                step_label     = "transform_encounters",
            ),
            TransformStep(
                procedure_name = "raw.sp_load_dim_patient",
                step_label     = "transform_patients_scd2",
            ),
            DQCheckStep(),
        ]
    )


def build_claims_pipeline() -> Pipeline:
    """Near-real-time pipeline: load claims JSON → merge → DQ check → audit."""
    return Pipeline(
        name  = "healthcare_claims_incremental",
        steps = [
            StageLoadStep(
                stage_name   = "raw.s3_stage_claims",
                target_table = "raw.claims_landing",
                file_pattern = "claims/",
            ),
            TransformStep(
                procedure_name = "raw.sp_load_fact_claim",
                step_label     = "transform_claims_merge",
            ),
            DQCheckStep(),
        ]
    )


# ── CLI entry point ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Run a named ETL pipeline.")
    parser.add_argument(
        "pipeline",
        choices=["encounters", "claims"],
        help="Which pipeline to run.",
    )
    args = parser.parse_args()

    pipelines = {
        "encounters": build_encounters_pipeline,
        "claims":     build_claims_pipeline,
    }

    result = pipelines[args.pipeline]().run()

    # Exit with non-zero code so schedulers (Airflow, cron) detect failures
    sys.exit(0 if result.status == PipelineStatus.SUCCESS else 1)
