# tests/unit/test_nba_feature_backfill_dag.py
import pandas as pd
from unittest.mock import MagicMock, patch


def _run(tmp_path, overwrite=None):
    import nba.dags.nba_feature_backfill_dag as dag_mod
    params = {"date_from": "2026-10-20", "date_to": "2026-10-21"}
    if overwrite is not None:
        params["overwrite"] = overwrite
    built = pd.DataFrame({"player_id": [1], "rolling_avg_5g": [9.0]})
    with patch.object(dag_mod, "FEATURES_DIR", str(tmp_path)), \
         patch.object(dag_mod, "get_data_db_conn", return_value=MagicMock()), \
         patch.object(dag_mod, "build_features", return_value=built) as build:
        dag_mod.run_backfill(params=params)
    return build


def _seed(tmp_path):
    existing = tmp_path / "props_features_2026-10-20.parquet"
    pd.DataFrame({"player_id": [1], "rolling_avg_5g": [1.0]}).to_parquet(existing, index=False)
    return existing


def test_feature_backfill_skips_existing_files_by_default(tmp_path):
    existing = _seed(tmp_path)
    build = _run(tmp_path)
    assert [c.args[1] for c in build.call_args_list] == ["2026-10-21"]
    assert pd.read_parquet(existing)["rolling_avg_5g"].tolist() == [1.0]


def test_feature_backfill_overwrite_rebuilds_existing_files(tmp_path):
    existing = _seed(tmp_path)
    build = _run(tmp_path, overwrite=True)
    assert [c.args[1] for c in build.call_args_list] == ["2026-10-20", "2026-10-21"]
    assert pd.read_parquet(existing)["rolling_avg_5g"].tolist() == [9.0]
    assert not list(tmp_path.glob("*.tmp"))
