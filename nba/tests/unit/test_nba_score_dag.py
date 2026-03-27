def test_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="nba/dags/", include_examples=False)
    assert "nba_score_dag" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag_has_expected_tasks():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="nba/dags/", include_examples=False).dags["nba_score_dag"]
    task_ids = {t.task_id for t in dag.tasks}
    assert "wait_for_nba_feature_dag" in task_ids
    assert "score" in task_ids


def test_score_task_depends_on_sensor():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="nba/dags/", include_examples=False).dags["nba_score_dag"]
    score_task = dag.get_task("score")
    upstream_ids = {t.task_id for t in score_task.upstream_list}
    assert "wait_for_nba_feature_dag" in upstream_ids


def test_sensor_targets_nba_feature_dag():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="nba/dags/", include_examples=False).dags["nba_score_dag"]
    sensor = dag.get_task("wait_for_nba_feature_dag")
    assert sensor.external_dag_id == "nba_feature_dag"


def test_dag_has_slack_callbacks():
    from airflow.models import DagBag
    from shared.plugins.slack_notifier import notify_score_ready, notify_failure
    dag = DagBag(dag_folder="nba/dags/", include_examples=False).dags["nba_score_dag"]
    assert dag.on_success_callback is notify_score_ready
    assert dag.on_failure_callback is notify_failure
