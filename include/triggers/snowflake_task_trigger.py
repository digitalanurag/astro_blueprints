from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

from include.utils.snowflake import ident_forms, validate_fq_task_name

_SUCCESS_STATES = {"SUCCEEDED"}

_FAILURE_STATES = {
    "FAILED",
    "CANCELLED",
    "SKIPPED",
    "FAILED_AND_AUTO_SUSPENDED",
}


class SnowflakeTaskTrigger(BaseTrigger):
    def __init__(
        self,
        fq_task_name: str,
        snowflake_conn_id: str,
        trigger_time_utc: str,
        poll_interval: float = 30,
    ) -> None:
        self.fq_task_name = validate_fq_task_name(fq_task_name)
        self.snowflake_conn_id = snowflake_conn_id
        self.trigger_time_utc = trigger_time_utc
        self.poll_interval = float(poll_interval)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "include.triggers.snowflake_task_trigger.SnowflakeTaskTrigger",
            {
                "fq_task_name": self.fq_task_name,
                "snowflake_conn_id": self.snowflake_conn_id,
                "trigger_time_utc": self.trigger_time_utc,
                "poll_interval": self.poll_interval,
            },
        )

    def _check_once(self) -> dict[str, Any]:
        db_part, schema_part, task_part = self.fq_task_name.split(".")

        db_sql, db_value = ident_forms(db_part)
        _, schema_value = ident_forms(schema_part)
        _, task_value = ident_forms(task_part)

        hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)

        context_rows = hook.get_records(
            "SELECT CURRENT_WAREHOUSE(), CURRENT_ROLE(), CURRENT_ACCOUNT()"
        )

        current_wh, _, _ = context_rows[0] if context_rows else (None, None, None)

        if not current_wh:
            return {
                "status": "failed",
                "state": "CONNECTION_ERROR",
                "error_code": None,
                "error_message": (
                    f"Snowflake connection '{self.snowflake_conn_id}' "
                    "has no active warehouse."
                ),
            }

        sql = (
            "SELECT "
            "NAME, "
            "DATABASE_NAME, "
            "SCHEMA_NAME, "
            "STATE, "
            "SCHEDULED_TIME, "
            "QUERY_START_TIME, "
            "COMPLETED_TIME, "
            "ERROR_CODE, "
            "ERROR_MESSAGE, "
            "QUERY_ID "
            f"FROM TABLE({db_sql}.INFORMATION_SCHEMA.TASK_HISTORY("
            "SCHEDULED_TIME_RANGE_START => "
            f"'{self.trigger_time_utc}'::TIMESTAMP_LTZ, "
            f"TASK_NAME => '{task_value}'"
            ")) "
            "WHERE DATABASE_NAME = %(db)s "
            "AND SCHEMA_NAME = %(schema)s "
            "ORDER BY SCHEDULED_TIME DESC "
            "LIMIT 1"
        )

        rows = hook.get_records(
            sql,
            parameters={"db": db_value, "schema": schema_value},
        )

        if not rows:
            return {"status": "waiting", "state": "PENDING"}

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

        result = {
            "status": "waiting",
            "fq_task_name": self.fq_task_name,
            "name": name,
            "database_name": db_name,
            "schema_name": schema_name,
            "state": state_upper,
            "scheduled_time": str(scheduled_time),
            "query_start_time": str(query_start_time),
            "completed_time": str(completed_time),
            "query_id": query_id,
            "error_code": error_code,
            "error_message": error_message,
        }

        if state_upper in _SUCCESS_STATES:
            result["status"] = "success"
        elif state_upper in _FAILURE_STATES:
            result["status"] = "failed"

        return result

    async def run(self) -> AsyncIterator[TriggerEvent]:
        while True:
            try:
                # SnowflakeHook is synchronous. Run the blocking Snowflake
                # call outside the Triggerer's event loop.
                result = await asyncio.to_thread(self._check_once)
            except Exception as exc:
                yield TriggerEvent(
                    {
                        "status": "failed",
                        "state": "TRIGGER_ERROR",
                        "error_code": None,
                        "error_message": str(exc),
                    }
                )
                return

            if result.get("status") in {"success", "failed"}:
                yield TriggerEvent(result)
                return

            await asyncio.sleep(self.poll_interval)


__all__ = ["SnowflakeTaskTrigger"]
