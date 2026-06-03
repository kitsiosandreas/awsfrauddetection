"""
Login Risk Model — Training Script
Model: RandomForest classifier on session behavioural features.

Features:
  - is_new_device          : 1 if device never seen on this account
  - is_new_country         : 1 if login country differs from all history
  - geo_velocity_kph       : Speed required to travel from last login location
  - hour_of_day            : Login hour (UTC)
  - failed_attempts_before : # failed attempts in prior 10 minutes
  - days_since_account_open: Account age in days
  - has_pending_withdrawal : 1 if withdrawal request in prior 24h
  - session_duration_min   : Session duration (0 for attack = very short)
  - password_reset_flag    : 1 if password was reset in prior 60 minutes
"""
import argparse
import json
import logging
import os
import pickle
import tarfile

import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import cross_val_score

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

FEATURES = [
    "is_new_device",
    "is_new_country",
    "geo_velocity_kph",
    "hour_of_day",
    "failed_attempts_before",
    "days_since_account_open",
    "has_pending_withdrawal",
    "session_duration_min",
    "password_reset_flag",
]

# Synthetic data distributions
NORMAL_DIST = {
    "is_new_device":           (0, 0),           # categorical 0/1
    "is_new_country":          (0, 0),
    "geo_velocity_kph":        (0, 50),           # local movement only
    "hour_of_day":             (8, 18),           # business hours
    "failed_attempts_before":  (0, 1),
    "days_since_account_open": (30, 1000),
    "has_pending_withdrawal":  (0, 0),
    "session_duration_min":    (15, 120),
    "password_reset_flag":     (0, 0),
}

ATTACK_DIST = {
    "is_new_device":           (1, 1),            # always new device
    "is_new_country":          (1, 1),            # always new country
    "geo_velocity_kph":        (1200, 8000),      # impossible travel
    "hour_of_day":             (0, 6),            # off hours
    "failed_attempts_before":  (5, 50),           # credential stuffing
    "days_since_account_open": (180, 1000),       # mature accounts targeted
    "has_pending_withdrawal":  (1, 1),
    "session_duration_min":    (1, 10),           # very short
    "password_reset_flag":     (1, 1),
}


def _sample_distribution(spec: dict, n: int, rng) -> np.ndarray:
    rows = []
    for feat in FEATURES:
        lo, hi = spec[feat]
        if lo == hi:  # categorical
            rows.append(np.full(n, lo, dtype=float))
        else:
            rows.append(rng.uniform(lo, hi, n))
    return np.column_stack(rows)


def _generate_data(n_normal=5000, n_fraud=1000, seed=42):
    rng = np.random.default_rng(seed)
    X_normal = _sample_distribution(NORMAL_DIST, n_normal, rng)
    X_fraud  = _sample_distribution(ATTACK_DIST, n_fraud, rng)
    X = np.vstack([X_normal, X_fraud])
    y = np.array([0] * n_normal + [1] * n_fraud)
    return X, y


def train(args):
    X, y = _generate_data(args.n_normal, args.n_fraud, args.seed)
    logger.info(f"Training LoginRisk on {len(X)} samples")

    model = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", RandomForestClassifier(
            n_estimators=300,
            max_depth=10,
            class_weight="balanced",
            random_state=args.seed,
            n_jobs=-1,
        )),
    ])
    model.fit(X, y)

    cv_scores = cross_val_score(model, X, y, cv=3, scoring="roc_auc")
    logger.info(f"CV AUC: {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")

    os.makedirs(args.model_dir, exist_ok=True)
    model_path = os.path.join(args.model_dir, "model.pkl")
    with open(model_path, "wb") as f:
        pickle.dump(model, f)

    meta = {
        "model_type": "RandomForestClassifier",
        "features": FEATURES,
        "cv_auc_mean": round(cv_scores.mean(), 4),
        "cv_auc_std":  round(cv_scores.std(), 4),
    }
    with open(os.path.join(args.model_dir, "model_meta.json"), "w") as f:
        json.dump(meta, f, indent=2)

    os.makedirs(args.output_dir, exist_ok=True)
    tar_path = os.path.join(args.output_dir, "model.tar.gz")
    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(model_path, arcname="model.pkl")
        tar.add(os.path.join(args.model_dir, "model_meta.json"), arcname="model_meta.json")
        inference_src = os.path.join(os.path.dirname(__file__), "inference.py")
        if os.path.exists(inference_src):
            tar.add(inference_src, arcname="inference.py")

    logger.info(f"Saved to {tar_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-dir",  default=os.environ.get("SM_MODEL_DIR", "/opt/ml/model"))
    parser.add_argument("--output-dir", default=os.environ.get("SM_OUTPUT_DIR", "/opt/ml/output"))
    parser.add_argument("--n-normal",   type=int, default=5000)
    parser.add_argument("--n-fraud",    type=int, default=1000)
    parser.add_argument("--seed",       type=int, default=42)
    args = parser.parse_args()
    train(args)
