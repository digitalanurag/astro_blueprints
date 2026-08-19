from __future__ import annotations
from airflow.sdk import TaskGroup
from pydantic import BaseModel, ConfigDict, Field, field_validator
from blueprint import Blueprint
from config.snowflake_task_history_trigger import (
    SnowflakeExecuteTaskOperator, SnowflakeTaskHistorySensor, validate_identifier,
)

class SnowflakeTriggerTaskConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    task_name: str
    database: str
    schema_: str = Field(alias="schema")
    snowflake_conn_id: str = "snowflake_conn"
    warehouse: str | None = None
    role: str | None = None
    poke_interval: int = Field(default=30, ge=5)
    process_timeout: int = Field(default=3600, ge=60)

    @field_validator("task_name", "database", "schema_")
    @classmethod
    def validate_required(cls, value: str, info) -> str:
        return validate_identifier(value, info.field_name)

class SnowflakeTriggerTask(Blueprint[SnowflakeTriggerTaskConfig]):
    def render(self, config: SnowflakeTriggerTaskConfig) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            execute = SnowflakeExecuteTaskOperator(
                task_id="execute", database=config.database, schema=config.schema_,
                task_name=config.task_name, snowflake_conn_id=config.snowflake_conn_id,
                warehouse=config.warehouse, role=config.role,
            )
            wait = SnowflakeTaskHistorySensor(
                task_id="wait", database=config.database, schema=config.schema_,
                task_name=config.task_name, snowflake_conn_id=config.snowflake_conn_id,
                warehouse=config.warehouse, role=config.role,
                trigger_task_id=f"{self.step_id}.execute",
                poke_interval=config.poke_interval, timeout=config.process_timeout, retries=0,
            )
            execute >> wait
        return group
