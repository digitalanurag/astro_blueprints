from __future__ import annotations

from airflow.sdk import TaskGroup
from blueprint import BaseModel, Blueprint, Field
from pydantic import field_validator

from include.operators.snowflake_operators import (
    SnowflakeExecuteTaskOperator,
    SnowflakeTaskCompletionSensor,
)
from include.utils.snowflake import validate_fq_task_name


def _task_id_from_fq_name(fq_task_name: str) -> str:
    return fq_task_name.replace('"', "").replace(".", "__").lower()


class SnowflakeTaskConfig(BaseModel):
    snowflake_conn_id: str = Field(
        default="snowflake_conn",
        description="Airflow connection id used to reach Snowflake.",
    )
    snowflake_task: str = Field(
        description=(
            "Fully qualified Snowflake task name in DATABASE.SCHEMA.TASK format."
        )
    )
    poll_interval_seconds: int = Field(
        default=30,
        ge=5,
        le=3600,
        description="Seconds between TASK_HISTORY checks while deferred.",
    )
    task_timeout_seconds: int = Field(
        default=3600,
        ge=60,
        description="Maximum seconds to wait for the Snowflake task.",
    )

    @field_validator("snowflake_task")
    @classmethod
    def _validate_snowflake_task(cls, value: str) -> str:
        return validate_fq_task_name(value)


class SnowflakeTaskBlueprint(Blueprint[SnowflakeTaskConfig]):
    def render(self, config: SnowflakeTaskConfig) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            slug = _task_id_from_fq_name(config.snowflake_task)

            trigger = SnowflakeExecuteTaskOperator(
                task_id=f"trigger__{slug}",
                fq_task_name=config.snowflake_task,
                snowflake_conn_id=config.snowflake_conn_id,
                retries=1,
            )

            monitor = SnowflakeTaskCompletionSensor(
                task_id=f"monitor__{slug}",
                fq_task_name=config.snowflake_task,
                snowflake_conn_id=config.snowflake_conn_id,
                trigger_task_id=f"{self.step_id}.trigger__{slug}",
                poke_interval=config.poll_interval_seconds,
                timeout=config.task_timeout_seconds,
                retries=0,
            )

            # Internal dependency of this Blueprint.
            trigger >> monitor

        return group


__all__ = [
    "SnowflakeTaskConfig",
    "SnowflakeTaskBlueprint",
]
