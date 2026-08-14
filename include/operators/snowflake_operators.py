"""Operators for the Snowflake Task -> dbt Cloud blueprint.

Provides:
- ``SnowflakeExecuteTaskOperator``: submits ``EXECUTE TASK <fq_name>`` and
  records the trigger time so a downstream monitor can correlate the run
  produced by *this* Airflow task instance.
- ``SnowflakeTaskCompletionSensor``: reschedule-mode sensor that polls
  ``INFORMATION_SCHEMA.TASK_HISTORY`` for the run scheduled at/after the
  trigger time and succeeds only when Snowflake reports SUCCEEDED. Failed,
  cancelled, or timed-out terminal states cause the sensor (and the DAG
  run) to fail with a descriptive error.

Both classes validate fully qualified Snowflake task names to guard against
SQL injection through Blueprint YAML configuration.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.bases.sensor import BaseSensorOperator, PokeReturnValue

# Terminal state buckets for TASK_HISTORY.STATE.
# https://docs.snowflake.com/en/sql-reference/functions/task_history
_SUCCESS_STATES = {"SUCCEEDED"}
_FAILURE_STATES = {"FAILED", "CANCELLED", "SKIPPED", "FAILED_AND_AUTO_SUSPENDED"}
_RUNNING_STATES = {"SCHEDULED", "EXECUTING"}

# Fully qualified Snowflake identifier: DB.SCHEMA.TASK.
# Each part is either an unquoted identifier (letters/digits/_/$) starting with
# a letter/underscore, or a double-quoted identifier (no embedded quotes).
_IDENT = r'(?:[A-Za-z_][A-Za-z0-9_$]*|"[^"]+")'
_FQ_TASK_RE = re.compile(rf"^{_IDENT}\.{_IDENT}\.{_IDENT}$")


def validate_fq_task_name(name: str) -> str:
    """Return ``name`` if it is a safe fully qualified Snowflake task name.

    Raises ``ValueError`` otherwise. This prevents arbitrary SQL from being
    injected via Blueprint YAML input.
    """
    if not isinstance(name, str) or not _FQ_TASK_RE.match(name):
        raise ValueError(
            f"Invalid fully qualified Snowflake task name: {name!r}. "
            "Expected DATABASE.SCHEMA.TASK using unquoted identifiers "
            "or double-quoted identifiers (e.g. POC_DB.PUBLIC.TASK_LOAD_ORDERS)."
        )
    return name


def _ident_forms(part: str) -> tuple[str, str]:
    """Return ``(sql_form, metadata_value)`` for a Snowflake identifier part.

    - Unquoted identifiers are folded to upper case in SQL and stored
      upper case in ``INFORMATION_SCHEMA`` metadata.
    - Double-quoted identifiers preserve case; the SQL form keeps the
      quotes, the metadata value strips them.

    Input is trusted only because :func:`validate_fq_task_name` restricts
    each part to a safe identifier regex.
    """
    if part.startswith('"') and part.endswith('"'):
        return part, part[1:-1]
    upper = part.upper()
    return upper, upper


def _split_fq_task_name(name: str) -> tuple[str, str, str]:
    """Split ``DB.SCHEMA.TASK`` into unquoted upper-case components."""
    parts: list[str] = []
    for part in name.split("."):
        _, value = _ident_forms(part)
        parts.append(value)
    return parts[0], parts[1], parts[2]


class SnowflakeExecuteTaskOperator(BaseOperator):
    """Submit ``EXECUTE TASK <fq_task_name>`` to Snowflake.

    Pushes a small correlation payload to XCom so the downstream monitor
    can look up the exact run this operator triggered:

    - ``fq_task_name``: full task identifier
    - ``trigger_time_utc``: ISO-8601 UTC timestamp (before EXECUTE TASK)
    - ``query_id``: Snowflake QUERY_ID returned by EXECUTE TASK (available
      via TASK_HISTORY.QUERY_ID for correlation as well).

    :param fq_task_name: Fully qualified Snowflake task ``DB.SCHEMA.TASK``.
    :param snowflake_conn_id: Airflow connection id for Snowflake.
    """

    ui_color = "#29B5E8"

    def __init__(
        self,
        fq_task_name: str,
        snowflake_conn_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.fq_task_name = validate_fq_task_name(fq_task_name)
        self.snowflake_conn_id = snowflake_conn_id

    def execute(self, context: Any) -> dict[str, str]:
        hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        # Record trigger time *before* firing EXECUTE TASK so the sensor can
        # filter TASK_HISTORY to runs scheduled at/after this moment.
        trigger_time = datetime.now(timezone.utc)
        trigger_time_iso = trigger_time.isoformat()

        self.log.info(
            "Snowflake Task: %s\nTrigger time: %s\nSubmitting EXECUTE TASK ...",
            self.fq_task_name,
            trigger_time_iso,
        )

        conn = hook.get_conn()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(f"EXECUTE TASK {self.fq_task_name}")
                query_id = getattr(cursor, "sfqid", None) or ""
            finally:
                cursor.close()
        finally:
            conn.close()

        self.log.info(
            "EXECUTE TASK submitted for %s (query_id=%s). Snowflake will run "
            "the task asynchronously; downstream sensor will poll for completion.",
            self.fq_task_name,
            query_id,
        )
        return {
            "fq_task_name": self.fq_task_name,
            "trigger_time_utc": trigger_time_iso,
            "query_id": query_id,
        }


class SnowflakeTaskCompletionSensor(BaseSensorOperator):
    """Poll Snowflake ``INFORMATION_SCHEMA.TASK_HISTORY`` for completion.

    Correlates to the run triggered by :class:`SnowflakeExecuteTaskOperator`
    by pulling ``trigger_time_utc`` from the upstream XCom and filtering
    ``TASK_HISTORY`` to runs with ``SCHEDULED_TIME >= trigger_time`` for the
    matching ``DATABASE_NAME``/``SCHEMA_NAME``/``NAME``.

    Use ``mode="reschedule"`` so workers are released between polls.

    :param fq_task_name: Fully qualified Snowflake task ``DB.SCHEMA.TASK``.
    :param snowflake_conn_id: Airflow connection id for Snowflake.
    :param trigger_task_id: ``task_id`` of the paired trigger operator.
    """

    ui_color = "#79C7E3"

    def __init__(
        self,
        fq_task_name: str,
        snowflake_conn_id: str,
        trigger_task_id: str,
        **kwargs: Any,
    ) -> None:
        # Reschedule mode: free the worker between polls. poke_interval and
        # timeout are provided by the Blueprint.
        kwargs.setdefault("mode", "reschedule")
        super().__init__(**kwargs)
        self.fq_task_name = validate_fq_task_name(fq_task_name)
        self.snowflake_conn_id = snowflake_conn_id
        self.trigger_task_id = trigger_task_id

    def poke(self, context: Any) -> bool | PokeReturnValue:
        ti = context["ti"]
        payload = ti.xcom_pull(task_ids=self.trigger_task_id)
        if not payload:
            raise AirflowException(
                f"No XCom payload from trigger task '{self.trigger_task_id}'. "
                "Cannot correlate Snowflake task run."
            )
        trigger_time_iso: str = payload["trigger_time_utc"]

        # Snowflake's INFORMATION_SCHEMA is per-database and only resolves
        # when the SQL text fully qualifies it. Build the database SQL form
        # from the already-validated FQ task name.
        db_part, schema_part, task_part = self.fq_task_name.split(".")
        db_sql, db_value = _ident_forms(db_part)
        _, schema_value = _ident_forms(schema_part)
        _, task_value = _ident_forms(task_part)

        hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        # Preflight: TASK_HISTORY requires a warehouse in session. If none is
        # bound to the connection, raise a clear error instead of a cryptic
        # Snowflake compile error.
        ctx_rows = hook.get_records(
            "SELECT CURRENT_WAREHOUSE(), CURRENT_ROLE(), CURRENT_ACCOUNT()"
        )
        current_wh, current_role, current_account = (
            ctx_rows[0] if ctx_rows else (None, None, None)
        )
        if not current_wh:
            raise AirflowException(
                "Snowflake connection '%s' has no active warehouse. Add a "
                "'warehouse' entry to the connection Extra (Environment "
                "Manager) so INFORMATION_SCHEMA.TASK_HISTORY can execute."
                % self.snowflake_conn_id
            )

        sql = (
            "SELECT NAME, DATABASE_NAME, SCHEMA_NAME, STATE, "
            "SCHEDULED_TIME, QUERY_START_TIME, COMPLETED_TIME, "
            "ERROR_CODE, ERROR_MESSAGE, QUERY_ID "
            # NOTE: TASK_HISTORY() requires *constant literal* arguments,
            # so SCHEDULED_TIME_RANGE_START and TASK_NAME are inlined rather
            # than bound. Both values are internally produced (a datetime we
            # generated and an identifier already validated by regex), so
            # inlining is safe.
            f"FROM TABLE({db_sql}.INFORMATION_SCHEMA.TASK_HISTORY("
            f"  SCHEDULED_TIME_RANGE_START => '{trigger_time_iso}'::TIMESTAMP_LTZ, "
            f"  TASK_NAME => '{task_value}' "
            ")) "
            "WHERE DATABASE_NAME = %(db)s AND SCHEMA_NAME = %(schema)s "
            "ORDER BY SCHEDULED_TIME DESC "
            "LIMIT 1"
        )
        params = {
            "db": db_value,
            "schema": schema_value,
        }
        self.log.info(
            "Polling Snowflake TASK_HISTORY (account=%s, warehouse=%s, role=%s)\nSQL: %s\nParams: %s",
            current_account,
            current_wh,
            current_role,
            sql,
            params,
        )
        rows = hook.get_records(sql, parameters=params)

        if not rows:
            self.log.info(
                "Snowflake Task: %s\nState: PENDING (no TASK_HISTORY row yet since %s)",
                self.fq_task_name,
                trigger_time_iso,
            )
            return False

        (
            name,
            db_name,
            schema_name,
            state,
            scheduled_time,
            query_start_time,
            completed_time,
            error_code,
            error_message,
            query_id,
        ) = rows[0]
        state_upper = (state or "").upper()

        self.log.info(
            "Snowflake Task: %s.%s.%s\nState: %s\nScheduled: %s\nStarted: %s\nCompleted: %s\nQuery ID: %s",
            db_name,
            schema_name,
            name,
            state_upper or "UNKNOWN",
            scheduled_time,
            query_start_time,
            completed_time,
            query_id,
        )

        if state_upper in _SUCCESS_STATES:
            self.log.info("Snowflake Task: %s\nState: SUCCEEDED", self.fq_task_name)
            return PokeReturnValue(
                is_done=True,
                xcom_value={
                    "fq_task_name": self.fq_task_name,
                    "state": state_upper,
                    "scheduled_time": str(scheduled_time),
                    "query_start_time": str(query_start_time),
                    "completed_time": str(completed_time),
                    "query_id": query_id,
                },
            )

        if state_upper in _FAILURE_STATES:
            msg = (
                f"Snowflake Task: {self.fq_task_name}\n"
                f"State: {state_upper}\n"
                f"Error code: {error_code}\n"
                f"Error message: {error_message}"
            )
            self.log.error(msg)
            # Fail the sensor (and DAG) without retries by raising a non-skip exception.
            raise AirflowException(msg)

        if state_upper in _RUNNING_STATES:
            return False

        # Unknown state -- log and continue polling; timeout will fire eventually.
        self.log.warning(
            "Unknown Snowflake TASK_HISTORY state '%s' for %s -- continuing to poll.",
            state_upper,
            self.fq_task_name,
        )
        return False


__all__ = [
    "SnowflakeExecuteTaskOperator",
    "SnowflakeTaskCompletionSensor",
    "validate_fq_task_name",
]
