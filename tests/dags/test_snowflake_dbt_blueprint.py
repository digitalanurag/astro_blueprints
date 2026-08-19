"""Unit tests for the Snowflake Task and dbt Cloud Model blueprints.

These tests validate the Blueprint contracts in isolation: they construct
the configs, render into a DAG, and assert the resulting graph and
validation rules. They do not touch Snowflake or dbt Cloud.
"""

from __future__ import annotations

import pytest
from airflow.sdk import DAG
from pydantic import ValidationError

from dags.templates.dbt_cloud_model_blueprint import (
    DbtCloudModelBlueprint,
    DbtCloudModelConfig,
)
from dags.templates.snowflake_task_blueprint import (
    SnowflakeTaskBlueprint,
    SnowflakeTaskConfig,
)


def _make_snowflake_blueprint(
    step_id: str = "snowflake_task",
) -> SnowflakeTaskBlueprint:
    bp = SnowflakeTaskBlueprint()
    # ``step_id`` is set by the loader in production; set it manually for tests.
    bp.step_id = step_id
    return bp


def _make_dbt_blueprint(step_id: str = "dbt_model") -> DbtCloudModelBlueprint:
    bp = DbtCloudModelBlueprint()
    bp.step_id = step_id
    return bp


def _render_snowflake(cfg: SnowflakeTaskConfig, step_id: str = "snowflake_task") -> DAG:
    with DAG(dag_id="test_snowflake_task", schedule=None) as dag:
        _make_snowflake_blueprint(step_id).render(cfg)
    return dag


def _render_dbt(cfg: DbtCloudModelConfig, step_id: str = "dbt_model") -> DAG:
    with DAG(dag_id="test_dbt_model", schedule=None) as dag:
        _make_dbt_blueprint(step_id).render(cfg)
    return dag


# ---------------------------------------------------------------------------
# SnowflakeTaskBlueprint
# ---------------------------------------------------------------------------


def test_snowflake_blueprint_renders_trigger_and_monitor():
    cfg = SnowflakeTaskConfig(snowflake_task="POC_DB.PUBLIC.TASK_A")
    dag = _render_snowflake(cfg)
    task_ids = set(dag.task_ids)
    assert "snowflake_task.trigger__poc_db__public__task_a" in task_ids
    assert "snowflake_task.monitor__poc_db__public__task_a" in task_ids


def test_snowflake_monitor_depends_on_trigger():
    cfg = SnowflakeTaskConfig(snowflake_task="POC_DB.PUBLIC.TASK_A")
    dag = _render_snowflake(cfg)
    monitor = dag.get_task("snowflake_task.monitor__poc_db__public__task_a")
    upstream_ids = {t.task_id for t in monitor.upstream_list}
    assert "snowflake_task.trigger__poc_db__public__task_a" in upstream_ids


def test_snowflake_monitor_trigger_task_id_matches_full_path():
    cfg = SnowflakeTaskConfig(snowflake_task="POC_DB.PUBLIC.TASK_A")
    dag = _render_snowflake(cfg)
    monitor = dag.get_task("snowflake_task.monitor__poc_db__public__task_a")
    # The sensor needs to xcom_pull from the fully-qualified trigger task_id.
    assert monitor.trigger_task_id == "snowflake_task.trigger__poc_db__public__task_a"


def test_snowflake_config_defaults():
    cfg = SnowflakeTaskConfig(snowflake_task="POC_DB.PUBLIC.TASK_A")
    assert cfg.snowflake_conn_id == "snowflake_conn"
    assert cfg.poll_interval_seconds == 30
    assert cfg.task_timeout_seconds == 3600


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
        SnowflakeTaskConfig(snowflake_task=bad_name)


@pytest.mark.parametrize(
    "bad_interval",
    [0, 4, 3601, 10_000],
)
def test_poll_interval_bounds_enforced(bad_interval):
    with pytest.raises(ValidationError):
        SnowflakeTaskConfig(
            snowflake_task="POC_DB.PUBLIC.TASK_A",
            poll_interval_seconds=bad_interval,
        )


# ---------------------------------------------------------------------------
# DbtCloudModelBlueprint
# ---------------------------------------------------------------------------


def test_dbt_blueprint_renders_run_job_operator():
    cfg = DbtCloudModelConfig(
        dbt_account_id=1,
        dbt_job_id=2,
        dbt_model="stg_customers",
    )
    dag = _render_dbt(cfg)
    assert "dbt_model" in dag.task_ids
    op = dag.get_task("dbt_model")
    assert op.job_id == 2
    assert op.account_id == 1
    assert op.dbt_cloud_conn_id == "astro_dbt_connn"
    assert op.steps_override == ["dbt build --select stg_customers"]
    assert op.wait_for_termination is True
    assert op.deferrable is True


def test_dbt_blueprint_uses_config_intervals_and_timeout():
    cfg = DbtCloudModelConfig(
        dbt_account_id=1,
        dbt_job_id=2,
        dbt_model="stg_customers",
        dbt_check_interval_seconds=90,
        dbt_timeout_seconds=3600,
    )
    dag = _render_dbt(cfg)
    op = dag.get_task("dbt_model")
    assert op.check_interval == 90
    assert op.timeout == 3600


@pytest.mark.parametrize("bad_model", ["", "   ", "\t\n"])
def test_empty_or_whitespace_dbt_model_rejected(bad_model):
    with pytest.raises(ValidationError):
        DbtCloudModelConfig(
            dbt_account_id=1,
            dbt_job_id=2,
            dbt_model=bad_model,
        )


def test_dbt_model_is_stripped():
    cfg = DbtCloudModelConfig(
        dbt_account_id=1,
        dbt_job_id=2,
        dbt_model="  stg_customers  ",
    )
    assert cfg.dbt_model == "stg_customers"


@pytest.mark.parametrize("bad_interval", [0, 5, 9])
def test_dbt_check_interval_lower_bound(bad_interval):
    with pytest.raises(ValidationError):
        DbtCloudModelConfig(
            dbt_account_id=1,
            dbt_job_id=2,
            dbt_model="stg_customers",
            dbt_check_interval_seconds=bad_interval,
        )


@pytest.mark.parametrize("bad_timeout", [0, 30, 59])
def test_dbt_timeout_lower_bound(bad_timeout):
    with pytest.raises(ValidationError):
        DbtCloudModelConfig(
            dbt_account_id=1,
            dbt_job_id=2,
            dbt_model="stg_customers",
            dbt_timeout_seconds=bad_timeout,
        )
