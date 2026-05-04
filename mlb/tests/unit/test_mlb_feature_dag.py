# mlb/tests/unit/test_mlb_feature_dag.py
"""Structural tests for mlb_feature_dag. Requires real Airflow installed
(DagBag). When run in an env where Airflow is stubbed, these fail at import
— same caveat as the other DagBag tests in this repo.
"""


def test_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="mlb/dags/", include_examples=False)
    assert "mlb_feature_dag" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag_has_expected_tasks():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="mlb/dags/", include_examples=False).dags["mlb_feature_dag"]
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {"wait_for_mlb_stats_pipeline", "build_features"}


def test_build_features_task_depends_on_sensor():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="mlb/dags/", include_examples=False).dags["mlb_feature_dag"]
    build = dag.get_task("build_features")
    upstream_ids = {t.task_id for t in build.upstream_list}
    assert "wait_for_mlb_stats_pipeline" in upstream_ids


def test_sensor_targets_mlb_stats_pipeline():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="mlb/dags/", include_examples=False).dags["mlb_feature_dag"]
    sensor = dag.get_task("wait_for_mlb_stats_pipeline")
    assert sensor.external_dag_id == "mlb_stats_pipeline"


def test_dag_has_slack_callbacks():
    from airflow.models import DagBag
    from shared.plugins.slack_notifier import notify_failure
    dag = DagBag(dag_folder="mlb/dags/", include_examples=False).dags["mlb_feature_dag"]
    assert dag.on_success_callback is None
    assert dag.on_failure_callback is notify_failure
