"""Snowflake Task -> dbt Cloud blueprint.

One reusable Blueprint that:

1. Triggers one or more Snowflake Tasks in parallel via ``EXECUTE TASK``.
2. Monitors each task via ``INFORMATION_SCHEMA.TASK_HISTORY`` correlated
   to the current DAG run (not a prior successful execution).
3. Only if *every* Snowflake Task completes with ``SUCCEEDED`` does it
   trigger an existing dbt Cloud production job with a ``steps_override``
   built from the configured dbt model selectors
   (``dbt build --select <models>``).

If any Snowflake Task fails, the workflow fails and dbt Cloud is not
triggered. Credentials come exclusively from Airflow connections.
"""

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
    """Turn ``POC_DB.PUBLIC.TASK_LOAD_ORDERS`` into a safe Airflow task id."""
    return fq_task_name.replace('"', "").replace(".", "__").lower()


def _build_dbt_steps_override(dbt_models: list[str]) -> list[str]:
    """Build the dbt Cloud ``steps_override`` list from configured selectors.

    Produces a single ``dbt build --select <space-separated selectors>`` step.
    """
    selectors = " ".join(dbt_models)
    return [f"dbt build --select {selectors}"]


class SnowflakeDbtConfig(BaseModel):
    """Configuration for the Snowflake Task -> dbt Cloud Blueprint."""

    snowflake_conn_id: str = Field(
        default="snowflake_conn",
        description="Airflow connection id used to reach Snowflake.",
    )
    snowflake_tasks: list[str] = Field(
        description=(
            "Fully qualified Snowflake task names to execute in parallel, "
            "e.g. ['POC_DB.PUBLIC.TASK_LOAD_CUSTOMERS', "
            "'POC_DB.PUBLIC.TASK_LOAD_ORDERS']. Each entry must be "
            "DATABASE.SCHEMA.TASK; identifiers are validated to prevent "
            "SQL injection via YAML."
        ),
        min_length=1,
    )
    dbt_cloud_conn_id: str = Field(
        default="astro_dbt_connn",
        description="Airflow connection id for dbt Cloud.",
    )
    dbt_account_id: int = Field(
        description="dbt Cloud account id that owns the target job.",
    )
    dbt_job_id: int = Field(
        description="dbt Cloud job id to trigger after all Snowflake tasks succeed.",
    )
    dbt_models: list[str] = Field(
        description=(
            "dbt model names or selectors passed to the dbt Cloud run via "
            "steps_override, e.g. ['stg_customers', 'stg_orders']. Rendered "
            "as `dbt build --select <models>`."
        ),
        min_length=1,
    )
    poll_interval_seconds: int = Field(
        default=30,
        ge=5,
        le=3600,
        description="Seconds between Snowflake TASK_HISTORY status polls.",
    )
    task_timeout_seconds: int = Field(
        default=60 * 60,
        ge=60,
        description=(
            "Maximum seconds to wait for any single Snowflake task to reach "
            "a terminal state before failing the workflow."
        ),
    )
    dbt_check_interval_seconds: int = Field(
        default=60,
        ge=10,
        description="Seconds between dbt Cloud run status checks.",
    )
    dbt_timeout_seconds: int = Field(
        default=60 * 60 * 2,
        ge=60,
        description="Maximum seconds to wait for the dbt Cloud run to finish.",
    )

    @field_validator("snowflake_tasks")
    @classmethod
    def _validate_snowflake_tasks(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("snowflake_tasks must contain at least one task.")
        for name in v:
            validate_fq_task_name(name)
        # Ensure uniqueness so we don't generate duplicate Airflow task ids.
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
    """Run Snowflake Tasks then a dbt Cloud job with a per-run selector override.

    Renders a TaskGroup with, for each configured Snowflake task, a
    ``trigger_*`` operator and a ``monitor_*`` reschedule-mode sensor.
    All monitors are wired as upstream dependencies of a single
    ``run_dbt_models`` task, so dbt runs only when every Snowflake task
    is SUCCEEDED.
    """

    def render(self, config: SnowflakeDbtConfig) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            monitor_tasks: list[Any] = []

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

            previous_dbt_task = None
 
            for index, model in enumerate(config.dbt_models):
 
                run_dbt_model = DbtCloudRunJobOperator(
                task_id=f"run_dbt_model_{index + 1}",
                dbt_cloud_conn_id=config.dbt_cloud_conn_id,
                account_id=config.dbt_account_id,
                job_id=config.dbt_job_id,
                steps_override=[
                    f"dbt build --select {model}"
                ],
                wait_for_termination=True,
                check_interval=config.dbt_check_interval_seconds,
                timeout=config.dbt_timeout_seconds,
                deferrable=True,
                trigger_reason=f"Triggered by Airflow for dbt model: {model}",
            )
 
    # First dbt model waits for all Snowflake tasks
            if previous_dbt_task is None:
                for monitor in monitor_tasks:
                    monitor >> run_dbt_model
        
            # Remaining models run one after another
            else:
                previous_dbt_task >> run_dbt_model
        
            previous_dbt_task = run_dbt_model

        return group


__all__ = ["SnowflakeDbtConfig", "SnowflakeDbt", "_build_dbt_steps_override"]
