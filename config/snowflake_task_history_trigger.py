from __future__ import annotations
import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator

from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.bases.sensor import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent

_SUCCESS_STATES = {"SUCCEEDED"}
_FAILURE_STATES = {"FAILED", "CANCELLED", "SKIPPED", "FAILED_AND_AUTO_SUSPENDED"}
_IDENTIFIER_PATTERN = re.compile(r'^(?:[A-Za-z_][A-Za-z0-9_$]*|"[^"]+")$')

def validate_identifier(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"Invalid Snowflake {field_name}: {value!r}")
    return value

def _sql_identifier(value: str) -> str:
    return value if value.startswith('"') and value.endswith('"') else value.upper()

def _metadata_identifier(value: str) -> str:
    return value[1:-1] if value.startswith('"') and value.endswith('"') else value.upper()

class SnowflakeExecuteTaskOperator(BaseOperator):
    ui_color = "#29B5E8"

    def __init__(self, *, database: str, schema: str, task_name: str,
                 snowflake_conn_id: str, warehouse: str | None = None,
                 role: str | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.database = validate_identifier(database, "database")
        self.schema = validate_identifier(schema, "schema")
        self.task_name = validate_identifier(task_name, "task_name")
        self.snowflake_conn_id = snowflake_conn_id
        self.warehouse = validate_identifier(warehouse, "warehouse") if warehouse else None
        self.role = validate_identifier(role, "role") if role else None

    @property
    def fq_task_name(self) -> str:
        return f"{_sql_identifier(self.database)}.{_sql_identifier(self.schema)}.{_sql_identifier(self.task_name)}"

    def execute(self, context: Any) -> dict[str, Any]:
        hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        trigger_time_iso = datetime.now(timezone.utc).isoformat()
        conn = hook.get_conn()
        try:
            cursor = conn.cursor()
            try:
                if self.role:
                    cursor.execute(f"USE ROLE {_sql_identifier(self.role)}")
                if self.warehouse:
                    cursor.execute(f"USE WAREHOUSE {_sql_identifier(self.warehouse)}")
                cursor.execute(f"EXECUTE TASK {self.fq_task_name}")
                query_id = getattr(cursor, "sfqid", None) or ""
            finally:
                cursor.close()
        finally:
            conn.close()
        return {
            "fq_task_name": self.fq_task_name,
            "trigger_time_utc": trigger_time_iso,
            "query_id": query_id,
        }

class SnowflakeTaskHistoryTrigger(BaseTrigger):
    def __init__(self, *, database: str, schema: str, task_name: str,
                 snowflake_conn_id: str, trigger_time_utc: str,
                 poke_interval: float = 30, warehouse: str | None = None,
                 role: str | None = None) -> None:
        self.database = validate_identifier(database, "database")
        self.schema = validate_identifier(schema, "schema")
        self.task_name = validate_identifier(task_name, "task_name")
        self.snowflake_conn_id = snowflake_conn_id
        self.trigger_time_utc = trigger_time_utc
        self.poke_interval = float(poke_interval)
        self.warehouse = validate_identifier(warehouse, "warehouse") if warehouse else None
        self.role = validate_identifier(role, "role") if role else None

    @property
    def fq_task_name(self) -> str:
        return f"{_sql_identifier(self.database)}.{_sql_identifier(self.schema)}.{_sql_identifier(self.task_name)}"

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "config.snowflake_task_history_trigger.SnowflakeTaskHistoryTrigger",
            {
                "database": self.database, "schema": self.schema,
                "task_name": self.task_name, "snowflake_conn_id": self.snowflake_conn_id,
                "trigger_time_utc": self.trigger_time_utc,
                "poke_interval": self.poke_interval, "warehouse": self.warehouse,
                "role": self.role,
            },
        )

    def _check_task_history(self) -> dict[str, Any]:
        hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        conn = hook.get_conn()
        try:
            cursor = conn.cursor()
            try:
                if self.role:
                    cursor.execute(f"USE ROLE {_sql_identifier(self.role)}")
                if self.warehouse:
                    cursor.execute(f"USE WAREHOUSE {_sql_identifier(self.warehouse)}")
                db_sql = _sql_identifier(self.database)
                db_meta = _metadata_identifier(self.database)
                schema_meta = _metadata_identifier(self.schema)
                task_meta = _metadata_identifier(self.task_name)
                sql = (
                    "SELECT NAME,DATABASE_NAME,SCHEMA_NAME,STATE,SCHEDULED_TIME,"
                    "QUERY_START_TIME,COMPLETED_TIME,ERROR_CODE,ERROR_MESSAGE,QUERY_ID "
                    f"FROM TABLE({db_sql}.INFORMATION_SCHEMA.TASK_HISTORY("
                    f"SCHEDULED_TIME_RANGE_START => '{self.trigger_time_utc}'::TIMESTAMP_LTZ,"
                    f"TASK_NAME => '{task_meta}')) "
                    "WHERE DATABASE_NAME = %s AND SCHEMA_NAME = %s "
                    "ORDER BY SCHEDULED_TIME DESC LIMIT 1"
                )
                cursor.execute(sql, (db_meta, schema_meta))
                row = cursor.fetchone()
            finally:
                cursor.close()
        finally:
            conn.close()

        if not row:
            return {"status": "waiting", "state": "PENDING"}

        name, db, schema, state, scheduled, started, completed, error_code, error_message, query_id = row
        state = (state or "").upper()
        result = {
            "status": "waiting", "fq_task_name": self.fq_task_name,
            "state": state, "scheduled_time": str(scheduled),
            "query_start_time": str(started), "completed_time": str(completed),
            "query_id": query_id, "error_code": error_code,
            "error_message": error_message,
        }
        if state in _SUCCESS_STATES:
            result["status"] = "success"
        elif state in _FAILURE_STATES:
            result["status"] = "failed"
        return result

    async def run(self) -> AsyncIterator[TriggerEvent]:
        while True:
            try:
                result = await asyncio.to_thread(self._check_task_history)
            except Exception as exc:
                yield TriggerEvent({"status": "failed", "state": "TRIGGER_ERROR",
                                    "error_code": None, "error_message": str(exc)})
                return
            if result.get("status") in {"success", "failed"}:
                yield TriggerEvent(result)
                return
            await asyncio.sleep(self.poke_interval)

class SnowflakeTaskHistorySensor(BaseSensorOperator):
    ui_color = "#79C7E3"

    def __init__(self, *, database: str, schema: str, task_name: str,
                 snowflake_conn_id: str, trigger_task_id: str,
                 warehouse: str | None = None, role: str | None = None,
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.database = validate_identifier(database, "database")
        self.schema = validate_identifier(schema, "schema")
        self.task_name = validate_identifier(task_name, "task_name")
        self.snowflake_conn_id = snowflake_conn_id
        self.trigger_task_id = trigger_task_id
        self.warehouse = validate_identifier(warehouse, "warehouse") if warehouse else None
        self.role = validate_identifier(role, "role") if role else None

    def execute(self, context: Any) -> Any:
        payload = context["ti"].xcom_pull(task_ids=self.trigger_task_id)
        if not payload:
            raise AirflowException(f"No XCom returned from {self.trigger_task_id}.")
        interval = self.poke_interval.total_seconds() if isinstance(self.poke_interval, timedelta) else float(self.poke_interval)
        timeout = self.timeout if isinstance(self.timeout, timedelta) else timedelta(seconds=float(self.timeout))
        self.defer(
            trigger=SnowflakeTaskHistoryTrigger(
                database=self.database, schema=self.schema, task_name=self.task_name,
                snowflake_conn_id=self.snowflake_conn_id,
                trigger_time_utc=payload["trigger_time_utc"],
                poke_interval=interval, warehouse=self.warehouse, role=self.role,
            ),
            method_name="execute_complete",
            timeout=timeout,
        )

    def execute_complete(self, context: Any, event: dict[str, Any] | None = None) -> dict[str, Any]:
        if event and event.get("status") == "success" and event.get("state") == "SUCCEEDED":
            return event
        event = event or {}
        raise AirflowException(
            f"Snowflake Task failed. State={event.get('state','UNKNOWN')}; "
            f"Error={event.get('error_message')}"
        )
