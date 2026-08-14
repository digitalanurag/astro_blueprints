"""Unit tests for the Snowflake Task -> dbt Cloud Blueprint.

These tests validate the Blueprint contract in isolation: they construct the
config, render the TaskGroup, and assert the resulting graph and validation
rules. They do not touch Snowflake or dbt Cloud.
"""

from __future__ import annotations

import pytest
from airflow.sdk import DAG
from pydantic import ValidationError

from dags.templates.snowflake_dbt_blueprints import (
    SnowflakeDbt,
    SnowflakeDbtConfig,
    _build_dbt_steps_override,
)


def _make_blueprint(step_id: str = "snowflake_to_dbt") -> SnowflakeDbt:
    bp = SnowflakeDbt()
    # ``step_id`` is set by the loader in production; set it manually for tests.
    bp.step_id = step_id
    return bp


def _render(cfg: SnowflakeDbtConfig, step_id: str = "snowflake_to_dbt"):
    with DAG(dag_id="test_snowflake_dbt", schedule=None) as dag:
        _make_blueprint(step_id).render(cfg)
    return dag


# ---------------------------------------------------------------------------
# 1. Renders successfully
# ---------------------------------------------------------------------------


def test_blueprint_renders_successfully():
    cfg = SnowflakeDbtConfig(
        snowflake_tasks=["POC_DB.PUBLIC.TASK_A"],
        dbt_account_id=1,
        dbt_job_id=2,
        dbt_models=["stg_customers"],
    )
    dag = _render(cfg)
    task_ids = set(dag.task_ids)
    assert "snowflake_to_dbt.trigger__poc_db__public__task_a" in task_ids
    assert "snowflake_to_dbt.monitor__poc_db__public__task_a" in task_ids
    assert "snowflake_to_dbt.run_dbt_models" in task_ids


# ---------------------------------------------------------------------------
# 2. One Snowflake task -> one trigger + one monitor
# ---------------------------------------------------------------------------


def test_single_snowflake_task_produces_one_trigger_and_monitor():
    cfg = SnowflakeDbtConfig(
        snowflake_tasks=["POC_DB.PUBLIC.TASK_ONLY"],
        dbt_account_id=1,
        dbt_job_id=2,
        dbt_models=["stg_customers"],
    )
    dag = _render(cfg)
    triggers = [t for t in dag.task_ids if ".trigger__" in t]
    monitors = [t for t in dag.task_ids if ".monitor__" in t]
    assert len(triggers) == 1
    assert len(monitors) == 1


# ---------------------------------------------------------------------------
# 3. Multiple Snowflake tasks -> correct number of trigger/monitor tasks
# ---------------------------------------------------------------------------


def test_multiple_snowflake_tasks_produce_correct_task_counts():
    cfg = SnowflakeDbtConfig(
        snowflake_tasks=[
            "POC_DB.PUBLIC.TASK_A",
            "POC_DB.PUBLIC.TASK_B",
            "POC_DB.PUBLIC.TASK_C",
        ],
        dbt_account_id=1,
        dbt_job_id=2,
        dbt_models=["stg_customers", "stg_orders"],
    )
    dag = _render(cfg)
    triggers = [t for t in dag.task_ids if ".trigger__" in t]
    monitors = [t for t in dag.task_ids if ".monitor__" in t]
    assert len(triggers) == 3
    assert len(monitors) == 3


# ---------------------------------------------------------------------------
# 4. dbt task depends on ALL monitor tasks
# ---------------------------------------------------------------------------


def test_dbt_task_depends_on_all_snowflake_monitors():
    cfg = SnowflakeDbtConfig(
        snowflake_tasks=[
            "POC_DB.PUBLIC.TASK_A",
            "POC_DB.PUBLIC.TASK_B",
        ],
        dbt_account_id=1,
        dbt_job_id=2,
        dbt_models=["stg_customers"],
    )
    dag = _render(cfg)
    dbt = dag.get_task("snowflake_to_dbt.run_dbt_models")
    upstream_ids = {t.task_id for t in dbt.upstream_list}
    assert "snowflake_to_dbt.monitor__poc_db__public__task_a" in upstream_ids
    assert "snowflake_to_dbt.monitor__poc_db__public__task_b" in upstream_ids


# ---------------------------------------------------------------------------
# 5. dbt cannot be reached without going through Snowflake monitors
# ---------------------------------------------------------------------------


def test_dbt_has_no_path_that_bypasses_snowflake_success():
    cfg = SnowflakeDbtConfig(
        snowflake_tasks=[
            "POC_DB.PUBLIC.TASK_A",
            "POC_DB.PUBLIC.TASK_B",
        ],
        dbt_account_id=1,
        dbt_job_id=2,
        dbt_models=["stg_customers"],
    )
    dag = _render(cfg)
    dbt = dag.get_task("snowflake_to_dbt.run_dbt_models")

    # Default trigger rule = all_success; never modified in the blueprint.
    assert dbt.trigger_rule == "all_success"

    # Every upstream is a monitor, and each monitor is reached only via its
    # trigger (no back-doors from any other task in the group).
    for monitor in dbt.upstream_list:
        assert monitor.task_id.startswith("snowflake_to_dbt.monitor__")
        trig_upstreams = {t.task_id for t in monitor.upstream_list}
        assert any(t.startswith("snowflake_to_dbt.trigger__") for t in trig_upstreams)


# ---------------------------------------------------------------------------
# 6. steps_override command is built correctly
# ---------------------------------------------------------------------------


def test_steps_override_command_built_from_models():
    steps = _build_dbt_steps_override(["stg_customers", "stg_orders"])
    assert steps == ["dbt build --select stg_customers stg_orders"]


def test_steps_override_included_on_dbt_operator():
    cfg = SnowflakeDbtConfig(
        snowflake_tasks=["POC_DB.PUBLIC.TASK_A"],
        dbt_account_id=1,
        dbt_job_id=2,
        dbt_models=["stg_customers", "stg_orders"],
    )
    dag = _render(cfg)
    dbt = dag.get_task("snowflake_to_dbt.run_dbt_models")
    assert dbt.steps_override == ["dbt build --select stg_customers stg_orders"]
    assert dbt.job_id == 2
    assert dbt.account_id == 1
    assert dbt.dbt_cloud_conn_id == "astro_dbt_connn"


# ---------------------------------------------------------------------------
# 7. Empty snowflake_tasks rejected
# ---------------------------------------------------------------------------


def test_empty_snowflake_tasks_rejected():
    with pytest.raises(ValidationError):
        SnowflakeDbtConfig(
            snowflake_tasks=[],
            dbt_account_id=1,
            dbt_job_id=2,
            dbt_models=["stg_customers"],
        )


# ---------------------------------------------------------------------------
# 8. Empty dbt_models rejected
# ---------------------------------------------------------------------------


def test_empty_dbt_models_rejected():
    with pytest.raises(ValidationError):
        SnowflakeDbtConfig(
            snowflake_tasks=["POC_DB.PUBLIC.TASK_A"],
            dbt_account_id=1,
            dbt_job_id=2,
            dbt_models=[],
        )


# ---------------------------------------------------------------------------
# Bonus: SQL-injection guard on Snowflake task names
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad_name",
    [
        "POC_DB.PUBLIC.TASK; DROP TABLE FOO",
        "POC_DB.PUBLIC",  # not fully qualified
        "POC_DB..TASK",
        "1BAD.PUBLIC.TASK",  # identifier cannot start with a digit
        "",
    ],
)
def test_invalid_snowflake_task_names_rejected(bad_name):
    with pytest.raises(ValidationError):
        SnowflakeDbtConfig(
            snowflake_tasks=[bad_name],
            dbt_account_id=1,
            dbt_job_id=2,
            dbt_models=["stg_customers"],
        )
