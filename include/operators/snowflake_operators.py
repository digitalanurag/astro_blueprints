from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.bases.sensor import BaseSensorOperator

from include.triggers.snowflake_task_trigger import SnowflakeTaskTrigger
from include.utils.snowflake import validate_fq_task_name


class SnowflakeExecuteTaskOperator(BaseOperator):
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

        # Record time BEFORE EXECUTE TASK.
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
            "EXECUTE TASK submitted for %s (query_id=%s).",
            self.fq_task_name,
            query_id,
        )

        return {
            "fq_task_name": self.fq_task_name,
            "trigger_time_utc": trigger_time_iso,
            "query_id": query_id,
        }


class SnowflakeTaskCompletionSensor(BaseSensorOperator):
    ui_color = "#79C7E3"

    def __init__(
        self,
        fq_task_name: str,
        snowflake_conn_id: str,
        trigger_task_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.fq_task_name = validate_fq_task_name(fq_task_name)
        self.snowflake_conn_id = snowflake_conn_id
        self.trigger_task_id = trigger_task_id

    def execute(self, context: Any) -> Any:
        payload = context["ti"].xcom_pull(task_ids=self.trigger_task_id)

        if not payload:
            raise AirflowException(
                f"No XCom payload from '{self.trigger_task_id}'. "
                "Cannot correlate Snowflake task run."
            )

        trigger_time_iso = payload["trigger_time_utc"]

        # Existing poke_interval configuration becomes our Trigger polling interval.
        if isinstance(self.poke_interval, timedelta):
            poll_interval = self.poke_interval.total_seconds()
        else:
            poll_interval = float(self.poke_interval)

        if isinstance(self.timeout, timedelta):
            defer_timeout = self.timeout
        else:
            defer_timeout = timedelta(seconds=float(self.timeout))

        self.log.info(
            "Deferring Snowflake task monitor for %s. "
            "Triggerer will check TASK_HISTORY every %s seconds.",
            self.fq_task_name,
            poll_interval,
        )

        # Worker is released here.
        self.defer(
            trigger=SnowflakeTaskTrigger(
                fq_task_name=self.fq_task_name,
                snowflake_conn_id=self.snowflake_conn_id,
                trigger_time_utc=trigger_time_iso,
                poll_interval=poll_interval,
            ),
            method_name="execute_complete",
            timeout=defer_timeout,
        )

    def execute_complete(
        self,
        context: Any,
        event: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not event:
            raise AirflowException(
                f"No event received while monitoring {self.fq_task_name}."
            )

        if event.get("status") == "success" and event.get("state") == "SUCCEEDED":
            self.log.info(
                "Snowflake Task: %s\nState: SUCCEEDED\nCompleted: %s",
                self.fq_task_name,
                event.get("completed_time"),
            )
            return event

        raise AirflowException(
            "Snowflake Task: %s\nState: %s\nError code: %s\nError message: %s"
            % (
                self.fq_task_name,
                event.get("state", "UNKNOWN"),
                event.get("error_code"),
                event.get("error_message"),
            )
        )


__all__ = [
    "SnowflakeExecuteTaskOperator",
    "SnowflakeTaskCompletionSensor",
]
