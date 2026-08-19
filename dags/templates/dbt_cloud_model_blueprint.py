from __future__ import annotations

from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from blueprint import BaseModel, Blueprint, Field
from pydantic import field_validator


class DbtCloudModelConfig(BaseModel):
    dbt_cloud_conn_id: str = Field(
        default="astro_dbt_connn",
        description="Airflow connection id for dbt Cloud.",
    )
    dbt_account_id: int = Field(description="dbt Cloud account id.")
    dbt_job_id: int = Field(description="Existing dbt Cloud job id.")
    dbt_model: str = Field(description="One dbt model name or selector.")
    dbt_check_interval_seconds: int = Field(
        default=60,
        ge=10,
        description="Seconds between dbt Cloud status checks.",
    )
    dbt_timeout_seconds: int = Field(
        default=7200,
        ge=60,
        description="Maximum seconds to wait for dbt Cloud.",
    )

    @field_validator("dbt_model")
    @classmethod
    def _validate_dbt_model(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("dbt_model must be a non-empty string.")
        return value.strip()


class DbtCloudModelBlueprint(Blueprint[DbtCloudModelConfig]):
    def render(self, config: DbtCloudModelConfig) -> DbtCloudRunJobOperator:
        return DbtCloudRunJobOperator(
            task_id=self.step_id,
            dbt_cloud_conn_id=config.dbt_cloud_conn_id,
            account_id=config.dbt_account_id,
            job_id=config.dbt_job_id,
            steps_override=[f"dbt build --select {config.dbt_model}"],
            wait_for_termination=True,
            check_interval=config.dbt_check_interval_seconds,
            timeout=config.dbt_timeout_seconds,
            # dbt provider already supports deferrable.
            deferrable=True,
            trigger_reason=(f"Triggered by Airflow for dbt model: {config.dbt_model}"),
        )


__all__ = [
    "DbtCloudModelConfig",
    "DbtCloudModelBlueprint",
]
