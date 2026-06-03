"""
Coordinated Trading Model — Training Script
Model: Isolation Forest on trade-timing and correlation features.
Runs as a SageMaker Training Job.

Features used:
  - trade_timing_entropy    : Shannon entropy of inter-trade intervals
  - instrument_concentration: Herfindahl index on instrument distribution
  - win_rate_7d             : Rolling 7-day win rate (>0.85 is suspicious)
  - avg_hold_duration_min   : Average position hold time in minutes
  - pnl_volatility_7d       : Volatility of P&L curve
  - correlated_accounts_cnt : # accounts with Pearson r > 0.85 vs this account
  - cluster_id_numeric      : Graph cluster membership (0 if none)
  - trade_count_24h         : Trades in last 24 hours
"""
import argparse
import json
import logging
import os
import pickle
import tarfile

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

FEATURES = [
    "trade_timing_entropy",
    "instrument_concentration",
    "win_rate_7d",
    "avg_hold_duration_min",
    "pnl_volatility_7d",
    "correlated_accounts_cnt",
    "trade_count_24h",
]

FRAUD_SIGNATURES = {
    # Coordinated ring member signatures for synthetic training data
    "coordinated": {
        "trade_timing_entropy":    (0.05, 0.20),  # very low entropy = regular intervals
        "instrument_concentration":(0.85, 1.0),   # always same instrument
        "win_rate_7d":             (0.80, 0.99),  # almost always profitable
        "avg_hold_duration_min":   (0.5,  5.0),   # very short holds
        "pnl_volatility_7d":       (0.01, 0.10),  # suspiciously stable P&L
        "correlated_accounts_cnt": (3,    20),     # many correlated peers
        "trade_count_24h":         (50,   200),    # high frequency
    },
    "normal": {
        "trade_timing_entropy":    (0.5,  1.5),
        "instrument_concentration":(0.15, 0.60),
        "win_rate_7d":             (0.40, 0.65),
        "avg_hold_duration_min":   (30,   480),
        "pnl_volatility_7d":       (0.15, 0.60),
        "correlated_accounts_cnt": (0,    1),
        "trade_count_24h":         (1,    15),
    },
}


def _generate_training_data(n_normal=5000, n_fraud=500, seed=42):
    """Generate synthetic labelled training data."""
    rng = np.random.default_rng(seed)

    def _sample(spec, n):
        rows = []
        for feat in FEATURES:
            lo, hi = spec[feat]
            rows.append(rng.uniform(lo, hi, n))
        return np.column_stack(rows)

    X_normal = _sample(FRAUD_SIGNATURES["normal"], n_normal)
    X_fraud  = _sample(FRAUD_SIGNATURES["coordinated"], n_fraud)
    X = np.vstack([X_normal, X_fraud])
    y = np.array([1] * n_normal + [-1] * n_fraud)  # IsolationForest convention
    return X, y


def train(args):
    X, y = _generate_training_data(
        n_normal=args.n_normal,
        n_fraud=args.n_fraud,
        seed=args.seed,
    )
    logger.info(f"Training on {len(X)} samples ({args.n_normal} normal, {args.n_fraud} fraud)")

    model = Pipeline([
        ("scaler", StandardScaler()),
        ("iso_forest", IsolationForest(
            n_estimators=200,
            contamination=args.n_fraud / (args.n_normal + args.n_fraud),
            max_features=len(FEATURES),
            random_state=args.seed,
            n_jobs=-1,
        )),
    ])
    model.fit(X)

    # Quick evaluation on training data
    scores = model.decision_function(X)
    preds  = model.predict(X)
    tp = np.sum((preds == -1) & (y == -1))
    fn = np.sum((preds ==  1) & (y == -1))
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    logger.info(f"Training recall on fraud: {recall:.3f}")

    # Persist model + metadata
    os.makedirs(args.model_dir, exist_ok=True)
    model_path = os.path.join(args.model_dir, "model.pkl")
    with open(model_path, "wb") as f:
        pickle.dump(model, f)

    meta = {
        "model_type": "IsolationForest",
        "features": FEATURES,
        "contamination": args.n_fraud / (args.n_normal + args.n_fraud),
        "recall_train": round(recall, 4),
    }
    with open(os.path.join(args.model_dir, "model_meta.json"), "w") as f:
        json.dump(meta, f, indent=2)

    # Package for SageMaker
    tar_path = os.path.join(args.output_dir, "model.tar.gz")
    os.makedirs(args.output_dir, exist_ok=True)
    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(model_path, arcname="model.pkl")
        tar.add(os.path.join(args.model_dir, "model_meta.json"), arcname="model_meta.json")
        # Include inference.py so SageMaker can load it
        inference_src = os.path.join(os.path.dirname(__file__), "inference.py")
        if os.path.exists(inference_src):
            tar.add(inference_src, arcname="inference.py")

    logger.info(f"Model saved to {tar_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-dir",  default=os.environ.get("SM_MODEL_DIR", "/opt/ml/model"))
    parser.add_argument("--output-dir", default=os.environ.get("SM_OUTPUT_DIR", "/opt/ml/output"))
    parser.add_argument("--n-normal",   type=int, default=5000)
    parser.add_argument("--n-fraud",    type=int, default=500)
    parser.add_argument("--seed",       type=int, default=42)
    args = parser.parse_args()
    train(args)
