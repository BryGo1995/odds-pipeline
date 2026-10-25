# nba/plugins/ml/train.py
"""
XGBoost training module for NBA player prop ML model.

train_model() reads all labeled Parquet files via DuckDB, trains a
market-anchored XGBoost model (MarketAnchoredModel), logs metrics to MLflow,
and tags the run as 'promotion_candidate' if ROC-AUC exceeds the current
production model.
"""
import logging
import os

import duckdb
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    log_loss,
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
MARKET_FEATURE = "implied_prob_over"
# Trees only nudge the market's probability, so they are kept tiny: 10 stumps
# with a low learning rate, each split needing ~400 props per side. The old
# 300 trees x depth 5 overfit ~1.5k props per prop type and scored below the
# market it was given as a feature (#36, #44).
XGB_PARAMS = {
    "n_estimators":     10,
    "max_depth":        1,
    "learning_rate":    0.03,
    "min_child_weight": 100,
    "subsample":        0.8,
    "colsample_bytree": 0.8,
    "eval_metric":      "logloss",
    "random_state":     42,
}
MODEL_NAME = "nba_prop_model"
VALIDATION_DAYS = 14  # held-out window for the logged metrics and promotion check
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
            f"SELECT * FROM read_parquet('{features_dir}/props_features_*.parquet', union_by_name=true) WHERE actual_result IS NOT NULL"
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
    # Missing values stay NaN: MarketAnchoredModel fills them with medians learned
    # on its training rows. Filling with this frame's own median leaked test and
    # scoring-day data into the inputs (#44).
    for col in ["rolling_avg_5g", "rolling_avg_10g", "rolling_avg_20g",
                "rolling_std_10g", "line_movement", "rest_days"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    use_features = features if features is not None else FEATURES
    X = df[use_features]
    if "actual_result" in df.columns and df["actual_result"].notna().all():
        y = df["actual_result"].astype(int)
    else:
        y = None
    return X, y, le


class MarketAnchoredModel:
    """
    XGBoost that starts from the market instead of from zero (#44).

    The market's Over probability is recalibrated with a two-parameter
    logistic fit (logit(implied_prob_over) -> outcome), which removes the vig
    and the books' Over bias. That logit is the XGBoost base_margin, so the
    trees only learn corrections from the other features. With no useful
    features the model reproduces the recalibrated market rather than falling
    below it.

    Missing feature values are filled with medians learned at fit time. If the
    market column is absent (e.g. a backtest ablation), the anchor is the
    training base rate.
    """

    def __init__(self, xgb_params: dict | None = None):
        self.xgb_params = dict(XGB_PARAMS if xgb_params is None else xgb_params)
        self.classes_ = np.array([0, 1])

    def fit(self, X: pd.DataFrame, y):
        y = np.asarray(y, dtype=int)
        self.feature_names_ = list(X.columns)
        self.tree_features_ = [c for c in X.columns if c != MARKET_FEATURE]
        self.fill_values_ = X[self.tree_features_].astype(float).median()
        if MARKET_FEATURE in X.columns:
            self.market_ = LogisticRegression(C=1e6).fit(_logit(X[MARKET_FEATURE]), y)
        else:
            self.market_ = None
            rate = np.clip(y.mean(), 1e-6, 1 - 1e-6)
            self.base_logit_ = float(np.log(rate / (1 - rate)))
        self.booster_ = xgb.XGBClassifier(**self.xgb_params)
        self.booster_.fit(self._tree_inputs(X), y, base_margin=self.market_margin(X))
        return self

    def market_margin(self, X: pd.DataFrame) -> np.ndarray:
        """Log-odds of the recalibrated market (or the base rate) for each row."""
        if self.market_ is None:
            return np.full(len(X), self.base_logit_)
        return self.market_.decision_function(_logit(X[MARKET_FEATURE]))

    def _tree_inputs(self, X: pd.DataFrame) -> pd.DataFrame:
        return X[self.tree_features_].astype(float).fillna(self.fill_values_)

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        X = X[self.feature_names_]
        p = self.booster_.predict_proba(self._tree_inputs(X), base_margin=self.market_margin(X))[:, 1]
        return np.column_stack([1 - p, p])

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return (self.predict_proba(X)[:, 1] >= 0.5).astype(int)

    @property
    def feature_importances_(self) -> dict[str, float]:
        """Tree importances for the non-market features."""
        return dict(zip(self.tree_features_, self.booster_.feature_importances_.tolist()))

    @property
    def market_coefficients(self) -> dict[str, float]:
        """Recalibration of logit(implied_prob_over): logit(p) = intercept + slope * logit(implied)."""
        if self.market_ is None:
            return {"intercept": self.base_logit_, "slope": 0.0}
        return {"intercept": float(self.market_.intercept_[0]), "slope": float(self.market_.coef_[0, 0])}


# _PlattCalibrator and _CalibratedModel are the pre-#44 recipe. They stay so
# production models registered before #44 can still be unpickled by score.py.
class _PlattCalibrator:
    """
    Sigmoid (Platt) calibration: logistic regression on the logit of the raw
    probability. Two parameters instead of an isotonic step function, so a
    small calibration window can't map a score range to exactly 0 or 1.
    """

    def fit(self, raw, y):
        self.lr = LogisticRegression(C=1e6).fit(_logit(raw), y)
        return self

    def predict(self, raw):
        return self.lr.predict_proba(_logit(raw))[:, 1]


def _logit(p) -> np.ndarray:
    p = np.clip(np.asarray(p, dtype=float), 1e-6, 1 - 1e-6)
    return np.log(p / (1 - p)).reshape(-1, 1)


class _CalibratedModel:
    """Wraps an XGBoost model + probability calibrator into a single sklearn-compatible object."""

    def __init__(self, base_model, calibrator):
        self.base_model = base_model
        self.calibrator = calibrator
        self.classes_ = base_model.classes_

    def __setstate__(self, state):
        # Models registered before #33 pickled an IsotonicRegression as `isotonic`
        if "isotonic" in state:
            state["calibrator"] = state.pop("isotonic")
        self.__dict__.update(state)

    def predict_proba(self, X):
        raw = self.base_model.predict_proba(X)[:, 1]
        calibrated = self.calibrator.predict(raw)
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

    # Fit on the training split to measure the held-out window, then refit on
    # all labeled rows for the registered model (the backtest does the same).
    y_prob = MarketAnchoredModel().fit(X_train, y_train).predict_proba(X_val)[:, 1]
    y_pred = (y_prob >= 0.5).astype(int)

    val_has_both_classes = len(np.unique(y_val)) >= 2
    roc_auc   = roc_auc_score(y_val, y_prob) if val_has_both_classes else float("nan")
    accuracy  = accuracy_score(y_val, y_pred)
    precision = precision_score(y_val, y_pred, zero_division=0)
    recall    = recall_score(y_val, y_pred, zero_division=0)
    brier     = brier_score_loss(y_val, y_prob) if val_has_both_classes else float("nan")
    val_log_loss = log_loss(y_val, y_prob, labels=[0, 1])
    implied_log_loss = log_loss(y_val, X_val[MARKET_FEATURE].astype(float), labels=[0, 1]) \
        if MARKET_FEATURE in features else float("nan")

    X_all, y_all, _ = prepare_features(df, features=features)
    model = MarketAnchoredModel().fit(X_all, y_all)

    prod_roc_auc = _get_production_model_auc(model_name)

    with mlflow.start_run() as run:
        mlflow.log_params({
            **{k: v for k, v in XGB_PARAMS.items()
               if k in ("n_estimators", "max_depth", "learning_rate", "min_child_weight",
                        "subsample", "colsample_bytree")},
            "recipe":           "market_anchored_xgb",
            "validation_days":  VALIDATION_DAYS,
            "train_rows":       len(train_df),
            "val_rows":         len(val_df),
            "final_fit_rows":   len(df),
        })
        mlflow.log_metrics({
            "roc_auc":   roc_auc,
            "accuracy":  accuracy,
            "precision": precision,
            "recall":    recall,
            "brier_score": brier,
            "log_loss":  val_log_loss,
            # Raw posted price (vig included); log_loss below this = beats the price
            "log_loss_implied": implied_log_loss,
            **{f"market_{k}": v for k, v in model.market_coefficients.items()},
        })
        if prod_roc_auc is not None:
            mlflow.log_metric("roc_auc_delta_vs_production", roc_auc - prod_roc_auc)

        mlflow.log_dict(model.feature_importances_, "feature_importance.json")
        mlflow.sklearn.log_model(model, artifact_path="model")

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
