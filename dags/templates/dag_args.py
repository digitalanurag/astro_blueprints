"""Project-wide DAG-level configuration for Blueprint YAML DAGs.

Adds support for `tags`, `start_date`, `catchup`, and `timezone` fields at the
top level of every `*.dag.yaml` file.
"""

from __future__ import annotations

from typing import Any

import pendulum
from pydantic import BaseModel, ConfigDict, Field, field_validator

from blueprint import BlueprintDagArgs


class ProjectDagArgsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schedule: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    # IANA timezone name. Defaults to Asia/Kolkata so schedules and start_date
    # are interpreted in IST rather than UTC.
    timezone: str = Field(default="Asia/Kolkata")
    # ISO-8601 date or datetime string (e.g. "2026-01-01" or "2026-01-01T00:00:00").
    # Interpreted in the configured `timezone`.
    start_date: str = Field(default="2026-01-01")
    catchup: bool = False

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        try:
            pendulum.timezone(value)
        except Exception as exc:  # pendulum raises InvalidTimezone
            raise ValueError(f"Invalid IANA timezone: {value!r}") from exc
        return value


class ProjectDagArgs(BlueprintDagArgs[ProjectDagArgsConfig]):
    """Emit DAG kwargs including tags, timezone-aware start_date, and catchup."""

    def render(self, config: ProjectDagArgsConfig) -> dict[str, Any]:
        tz = pendulum.timezone(config.timezone)
        start_date = pendulum.parse(config.start_date, tz=tz)

        kwargs: dict[str, Any] = {
            "tags": config.tags,
            "start_date": start_date,
            "catchup": config.catchup,
        }
        if config.schedule is not None:
            kwargs["schedule"] = config.schedule
        if config.description is not None:
            kwargs["description"] = config.description
        return kwargs
