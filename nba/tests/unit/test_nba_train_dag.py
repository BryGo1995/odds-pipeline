def test_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="nba/dags/", include_examples=False)
    assert "nba_train_dag" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag_has_train_model_task():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="nba/dags/", include_examples=False).dags["nba_train_dag"]
    task_ids = {t.task_id for t in dag.tasks}
    assert "train_model" in task_ids


def test_dag_has_slack_callbacks():
    from airflow.models import DagBag
    from shared.plugins.slack_notifier import notify_success, notify_failure
    dag = DagBag(dag_folder="nba/dags/", include_examples=False).dags["nba_train_dag"]
    assert dag.on_success_callback is notify_success
    assert dag.on_failure_callback is notify_failure
