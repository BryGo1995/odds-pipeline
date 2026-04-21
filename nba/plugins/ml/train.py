# nba/plugins/ml/train.py
"""
XGBoost training module for NBA player prop ML model.

train_model() reads all labeled Parquet files via DuckDB, trains a calibrated
XGBoost classifier, logs metrics to MLflow, and tags the run as
'promotion_candidate' if ROC-AUC exceeds the current production model.
"""
import logging
import os

import duckdb
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.preprocessing import LabelEncoder

from nba.plugins.transformers.features import PROP_STAT_MAP

FEATURES = [
    "implied_prob_over",
    "line_movement",
    "rolling_avg_5g",
    "rolling_avg_10g",
    "rolling_avg_20g",
    "rolling_std_10g",
    "is_home",
    "rest_days",
    "prop_type_encoded",
]
PER_PROP_FEATURES = [
    "implied_prob_over",
    "line_movement",
    "rolling_avg_5g",
    "rolling_avg_10g",
    "rolling_avg_20g",
    "rolling_std_10g",
    "is_home",
    "rest_days",
]
MODEL_NAME = "nba_prop_model"
VALIDATION_DAYS = 2
FEATURES_DIR = os.environ.get("FEATURES_DIR", "/data/features")
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")


def _make_label_encoder() -> LabelEncoder:
    """Deterministic encoder — always fitted on the full known prop type list."""
    le = LabelEncoder()
    le.fit(sorted(PROP_STAT_MAP.keys()))
    return le


def load_training_data(features_dir: str) -> pd.DataFrame:
    """Load all Parquet feature files and return only rows with actual_result populated."""
    conn = duckdb.connect()
    try:
        df = conn.execute(
            f"SELECT * FROM read_parquet('{features_dir}/*.parquet', union_by_name=true) WHERE actual_result IS NOT NULL"
        ).df()
    finally:
        conn.close()
    return df.dropna(subset=["actual_result"])


def prepare_features(df: pd.DataFrame, features: list[str] | None = None) -> tuple:
    """
    Encode categorical features and fill NAs. Returns (X, y, label_encoder).
    Uses a deterministic LabelEncoder fitted on the full known prop type list.
    """
    le = _make_label_encoder()
    df = df.copy()
    if features is None or "prop_type_encoded" in features:
        df["prop_type_encoded"] = le.transform(
            df["prop_type"].map(lambda x: x if x in le.classes_ else le.classes_[0])
        )
    df["is_home"] = df["is_home"].astype(object).fillna(0.5).astype(float)
    for col in ["rolling_avg_5g", "rolling_avg_10g", "rolling_avg_20g",
                "rolling_std_10g", "line_movement", "rest_days"]:
        if col in df.columns:
            numeric = pd.to_numeric(df[col], errors="coerce")
            df[col] = numeric.fillna(numeric.median())
    use_features = features if features is not None else FEATURES
    X = df[use_features]
    if "actual_result" in df.columns and df["actual_result"].notna().all():
        y = df["actual_result"].astype(int)
    else:
        y = None
    return X, y, le


class _CalibratedModel:
    """Wraps an XGBoost model + isotonic calibrator into a single sklearn-compatible object."""

    def __init__(self, base_model, isotonic: IsotonicRegression):
        self.base_model = base_model
        self.isotonic = isotonic
        self.classes_ = base_model.classes_

    def predict_proba(self, X):
        raw = self.base_model.predict_proba(X)[:, 1]
        calibrated = self.isotonic.predict(raw)
        return np.column_stack([1 - calibrated, calibrated])

    def predict(self, X):
        proba = self.predict_proba(X)[:, 1]
        return (proba >= 0.5).astype(int)


def train_model(features_dir: str = FEATURES_DIR, prop_type: str | None = None) -> str:
    """
    Train and log a new XGBoost model. Tags the MLflow run as 'promotion_candidate'
    if ROC-AUC exceeds the current production model.

    If prop_type is given, trains only on rows of that prop type and registers
    the model as nba_prop_model_{prop_type}.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    df = load_training_data(features_dir)

    if prop_type is not None:
        df = df[df["prop_type"] == prop_type]
        model_name = f"{MODEL_NAME}_{prop_type}"
        features = PER_PROP_FEATURES
        min_rows = 50
    else:
        model_name = MODEL_NAME
        features = FEATURES
        min_rows = 100

    if df.empty or len(df) < min_rows:
        raise ValueError(f"Insufficient training data: {len(df)} labeled rows (minimum {min_rows})")

    df = df.sort_values("game_date")
    cutoff = pd.to_datetime(df["game_date"].max()) - pd.Timedelta(days=VALIDATION_DAYS)
    train_df = df[pd.to_datetime(df["game_date"]) <= cutoff]
    val_df   = df[pd.to_datetime(df["game_date"]) >  cutoff]

    log = logging.getLogger(__name__)
    log.info(
        "Training split: %d total labeled rows | cutoff=%s | train=%d | val=%d",
        len(df), cutoff.date(), len(train_df), len(val_df),
    )

    if train_df.empty:
        raise ValueError(
            f"Training set is empty — all {len(df)} labeled rows fall within the "
            f"{VALIDATION_DAYS}-day validation window (cutoff={cutoff.date()}). "
            "Run the feature backfill for older dates to generate training data."
        )
    if val_df.empty:
        raise ValueError("Validation set is empty — add more recent data before training")

    X_train, y_train, _ = prepare_features(train_df, features=features)
    X_val,   y_val,   _ = prepare_features(val_df, features=features)

    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="logloss",
        random_state=42,
    )
    model.fit(X_train, y_train)

    # Calibrate probabilities — critical for reliable edge = model_prob - implied_prob
    # Manual isotonic calibration to avoid sklearn 1.6 FrozenEstimator/__sklearn_tags__ bug
    val_has_both_classes = len(np.unique(y_val)) >= 2
    n_val = len(y_val)
    min_per_class = min(np.unique(y_val, return_counts=True)[1])
    can_calibrate = val_has_both_classes and min_per_class >= 2
    if can_calibrate:
        raw_prob = model.predict_proba(X_val)[:, 1]
        iso = IsotonicRegression(y_min=0, y_max=1, out_of_bounds="clip")
        iso.fit(raw_prob, y_val)
        y_prob = iso.predict(raw_prob)
        calibrated = _CalibratedModel(model, iso)
    else:
        log.warning(
            "Validation set too small for calibration (n=%d, both_classes=%s) "
            "— skipping isotonic calibration, using raw model probabilities.",
            n_val, val_has_both_classes,
        )
        y_prob = model.predict_proba(X_val)[:, 1]
        calibrated = model

    y_pred = (y_prob >= 0.5).astype(int)

    roc_auc   = roc_auc_score(y_val, y_prob) if val_has_both_classes else float("nan")
    accuracy  = accuracy_score(y_val, y_pred)
    precision = precision_score(y_val, y_pred, zero_division=0)
    recall    = recall_score(y_val, y_pred, zero_division=0)
    brier     = brier_score_loss(y_val, y_prob) if val_has_both_classes else float("nan")

    prod_roc_auc = _get_production_model_auc(model_name)

    with mlflow.start_run() as run:
        mlflow.log_params({
            **{k: v for k, v in model.get_params().items()
               if k in ("n_estimators", "max_depth", "learning_rate", "subsample", "colsample_bytree")},
            "validation_days":  VALIDATION_DAYS,
            "train_rows":       len(train_df),
            "val_rows":         len(val_df),
        })
        mlflow.log_metrics({
            "roc_auc":   roc_auc,
            "accuracy":  accuracy,
            "precision": precision,
            "recall":    recall,
            "brier_score": brier,
        })
        if prod_roc_auc is not None:
            mlflow.log_metric("roc_auc_delta_vs_production", roc_auc - prod_roc_auc)

        importance = dict(zip(features, model.feature_importances_.tolist()))
        mlflow.log_dict(importance, "feature_importance.json")
        mlflow.sklearn.log_model(calibrated, artifact_path="model")

        model_uri = f"runs:/{run.info.run_id}/model"
        mlflow.register_model(model_uri, model_name)

        if prod_roc_auc is None or roc_auc > prod_roc_auc:
            delta_str = f"+{roc_auc - prod_roc_auc:.4f}" if prod_roc_auc is not None else "baseline"
            mlflow.set_tag("promotion_candidate", "true")
            mlflow.set_tag("promotion_note",
                           f"ROC-AUC {roc_auc:.4f} ({delta_str} vs production)")
        else:
            mlflow.set_tag("promotion_candidate", "false")
            mlflow.set_tag("promotion_note",
                           f"ROC-AUC {roc_auc:.4f} — no improvement over production ({prod_roc_auc:.4f})")

    return run.info.run_id


def _get_production_model_auc(model_name: str = MODEL_NAME) -> float | None:
    """Return the ROC-AUC of the current production model, or None if none exists."""
    try:
        client = mlflow.tracking.MlflowClient()
        mv = client.get_model_version_by_alias(model_name, "production")
        run = mlflow.get_run(mv.run_id)
        auc = run.data.metrics.get("roc_auc")
        return float(auc) if auc is not None else None
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "Could not retrieve production model AUC (treating as no production model): %s", exc
        )
        return None


def train_all_models(features_dir: str = FEATURES_DIR) -> dict[str, str]:
    """
    Train one model per prop type. Returns dict of {prop_type: mlflow_run_id}.
    Skips prop types with insufficient training data.
    """
    log = logging.getLogger(__name__)
    results = {}
    for prop_type in sorted(PROP_STAT_MAP.keys()):
        try:
            run_id = train_model(features_dir, prop_type=prop_type)
            results[prop_type] = run_id
        except ValueError as exc:
            log.warning("Skipping %s: %s", prop_type, exc)
    return results
