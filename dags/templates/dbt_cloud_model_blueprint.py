from __future__ import annotations
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.sdk import TaskGroup
from pydantic import BaseModel, ConfigDict, Field, field_validator
from blueprint import Blueprint

class DbtCloudModelConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    dbt_cloud_conn_id: str = "astro_dbt_connn"
    dbt_account_id: int
    dbt_job_id: int
    dbt_model: str
    dbt_check_interval: int = Field(default=60, ge=10)
    dbt_timeout: int = Field(default=7200, ge=60)

    @field_validator("dbt_model")
    @classmethod
    def validate_model(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("dbt_model cannot be empty")
        return value.strip()

class DbtCloudModel(Blueprint[DbtCloudModelConfig]):
    def render(self, config: DbtCloudModelConfig) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            DbtCloudRunJobOperator(
                task_id="run", dbt_cloud_conn_id=config.dbt_cloud_conn_id,
                account_id=config.dbt_account_id, job_id=config.dbt_job_id,
                steps_override=[f"dbt build --select {config.dbt_model}"],
                wait_for_termination=True, check_interval=config.dbt_check_interval,
                timeout=config.dbt_timeout, deferrable=True,
                trigger_reason=f"Triggered by Airflow for dbt model: {config.dbt_model}",
            )
        return group
