from __future__ import annotations

from typing import Any

from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.sdk import TaskGroup
from pydantic import field_validator

from blueprint import BaseModel, Blueprint, Field
from include.operators.snowflake_operators import (
    SnowflakeExecuteTaskOperator,
    SnowflakeTaskCompletionSensor,
    validate_fq_task_name,
)


def _task_id_from_fq_name(fq_task_name: str) -> str:
    return fq_task_name.replace('"', "").replace(".", "__").lower()


def _build_dbt_steps_override(dbt_models: list[str]) -> list[str]:
    selectors = " ".join(dbt_models)
    return [f"dbt build --select {selectors}"]


class SnowflakeDbtConfig(BaseModel):
    snowflake_conn_id: str = Field(default="snowflake_conn")

    snowflake_tasks: list[str] = Field(
        description="Fully qualified Snowflake task names.",
        min_length=1,
    )

    dbt_cloud_conn_id: str = Field(default="astro_dbt_connn")
    dbt_account_id: int = Field(description="dbt Cloud account id.")
    dbt_job_id: int = Field(description="dbt Cloud job id.")

    dbt_models: list[str] = Field(
        description="dbt models/selectors to run sequentially.",
        min_length=1,
    )

    poll_interval_seconds: int = Field(default=30, ge=5, le=3600)
    task_timeout_seconds: int = Field(default=3600, ge=60)
    dbt_check_interval_seconds: int = Field(default=60, ge=10)
    dbt_timeout_seconds: int = Field(default=7200, ge=60)

    @field_validator("snowflake_tasks")
    @classmethod
    def _validate_snowflake_tasks(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("snowflake_tasks must contain at least one task.")
        for name in v:
            validate_fq_task_name(name)
        if len(set(v)) != len(v):
            raise ValueError("snowflake_tasks contains duplicates.")
        return v

    @field_validator("dbt_models")
    @classmethod
    def _validate_dbt_models(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("dbt_models must contain at least one model or selector.")
        for model in v:
            if not isinstance(model, str) or not model.strip():
                raise ValueError("dbt_models entries must be non-empty strings.")
        return v


class SnowflakeDbt(Blueprint[SnowflakeDbtConfig]):

    def render(self, config: SnowflakeDbtConfig) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            monitor_tasks: list[Any] = []

            # 1. Trigger and monitor Snowflake tasks.
            for fq_task_name in config.snowflake_tasks:
                slug = _task_id_from_fq_name(fq_task_name)

                trigger = SnowflakeExecuteTaskOperator(
                    task_id=f"trigger__{slug}",
                    fq_task_name=fq_task_name,
                    snowflake_conn_id=config.snowflake_conn_id,
                    retries=1,
                )

                monitor = SnowflakeTaskCompletionSensor(
                    task_id=f"monitor__{slug}",
                    fq_task_name=fq_task_name,
                    snowflake_conn_id=config.snowflake_conn_id,
                    trigger_task_id=f"{self.step_id}.trigger__{slug}",
                    poke_interval=config.poll_interval_seconds,
                    timeout=config.task_timeout_seconds,
                    retries=0,
                )

                trigger >> monitor
                monitor_tasks.append(monitor)

            # 2. Run dbt models sequentially.
            previous_dbt_task = None

            for index, model in enumerate(config.dbt_models):
                run_dbt_model = DbtCloudRunJobOperator(
                    task_id=f"run_dbt_model_{index + 1}",
                    dbt_cloud_conn_id=config.dbt_cloud_conn_id,
                    account_id=config.dbt_account_id,
                    job_id=config.dbt_job_id,
                    steps_override=[f"dbt build --select {model}"],
                    wait_for_termination=True,
                    check_interval=config.dbt_check_interval_seconds,
                    timeout=config.dbt_timeout_seconds,
                    deferrable=True,
                    trigger_reason=f"Triggered by Airflow for dbt model: {model}",
                )

                # First dbt model waits for ALL Snowflake monitors.
                if previous_dbt_task is None:
                    for monitor in monitor_tasks:
                        monitor >> run_dbt_model
                else:
                    # Later models wait for the previous dbt model.
                    previous_dbt_task >> run_dbt_model

                previous_dbt_task = run_dbt_model

        return group


__all__ = ["SnowflakeDbtConfig", "SnowflakeDbt", "_build_dbt_steps_override"]
