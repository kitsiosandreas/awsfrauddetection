"""
Registration Anomaly / Bot Detection Model — Training Script
Model: Autoencoder (sklearn MLPRegressor used as a reconstruction model)
       + threshold on reconstruction error.

Features:
  - ip_registration_count   : # registrations from same /24 IP in 24h
  - device_fp_count          : # accounts sharing same canvas fingerprint
  - kyc_similarity_score     : Max fuzzy-match score vs existing accounts
  - registration_hour        : Hour of day (0-23)
  - form_fill_time_sec       : Time to complete registration form (bots are fast)
  - email_domain_risk        : 0=legitimate, 1=disposable domain
  - api_rate_per_min         : API calls per minute during registration
  - order_cancel_ratio       : Order/cancel ratio in first session (0-1)
"""
import argparse
import json
import logging
import os
import pickle
import tarfile

import numpy as np
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import Pipeline

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

FEATURES = [
    "ip_registration_count",
    "device_fp_count",
    "kyc_similarity_score",
    "registration_hour",
    "form_fill_time_sec",
    "email_domain_risk",
    "api_rate_per_min",
    "order_cancel_ratio",
]

NORMAL_DIST = {
    "ip_registration_count":  (1, 2),
    "device_fp_count":        (1, 1),
    "kyc_similarity_score":   (0.0, 0.2),
    "registration_hour":      (8, 20),
    "form_fill_time_sec":     (120, 900),
    "email_domain_risk":      (0.0, 0.1),
    "api_rate_per_min":       (1, 10),
    "order_cancel_ratio":     (0.1, 0.4),
}

ABUSE_DIST = {
    "ip_registration_count":  (10, 100),
    "device_fp_count":        (5, 50),
    "kyc_similarity_score":   (0.7, 0.99),
    "registration_hour":      (0, 5),
    "form_fill_time_sec":     (2, 20),
    "email_domain_risk":      (0.9, 1.0),
    "api_rate_per_min":       (50, 500),
    "order_cancel_ratio":     (0.75, 1.0),
}


def _sample(spec, n, rng):
    rows = [rng.uniform(*spec[f], n) for f in FEATURES]
    return np.column_stack(rows)


def train(args):
    rng = np.random.default_rng(args.seed)
    X_normal = _sample(NORMAL_DIST, args.n_normal, rng)
    X_abuse  = _sample(ABUSE_DIST,  args.n_fraud,  rng)

    # Autoencoder trained ONLY on normal data — anomaly = high reconstruction error
    scaler = MinMaxScaler()
    X_norm_scaled = scaler.fit_transform(X_normal)

    n_features = len(FEATURES)
    bottleneck = max(2, n_features // 2)

    autoencoder = MLPRegressor(
        hidden_layer_sizes=(n_features, bottleneck, n_features),
        activation="tanh",
        max_iter=500,
        random_state=args.seed,
        early_stopping=True,
        validation_fraction=0.1,
    )
    autoencoder.fit(X_norm_scaled, X_norm_scaled)

    # Determine threshold from normal data reconstruction errors
    recon_normal = autoencoder.predict(X_norm_scaled)
    errors_normal = np.mean((X_norm_scaled - recon_normal) ** 2, axis=1)

    X_abuse_scaled = scaler.transform(X_abuse)
    recon_abuse = autoencoder.predict(X_abuse_scaled)
    errors_abuse = np.mean((X_abuse_scaled - recon_abuse) ** 2, axis=1)

    # Threshold at 99th percentile of normal errors
    threshold = float(np.percentile(errors_normal, 99))
    recall = float(np.mean(errors_abuse > threshold))
    logger.info(f"Threshold: {threshold:.4f} | Fraud recall: {recall:.3f}")

    os.makedirs(args.model_dir, exist_ok=True)
    artifact = {
        "scaler": scaler,
        "autoencoder": autoencoder,
        "threshold": threshold,
    }
    model_path = os.path.join(args.model_dir, "model.pkl")
    with open(model_path, "wb") as f:
        pickle.dump(artifact, f)

    meta = {
        "model_type": "Autoencoder (MLPRegressor)",
        "features": FEATURES,
        "threshold": threshold,
        "fraud_recall_train": round(recall, 4),
    }
    with open(os.path.join(args.model_dir, "model_meta.json"), "w") as f:
        json.dump(meta, f, indent=2)

    os.makedirs(args.output_dir, exist_ok=True)
    tar_path = os.path.join(args.output_dir, "model.tar.gz")
    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(model_path, arcname="model.pkl")
        tar.add(os.path.join(args.model_dir, "model_meta.json"), arcname="model_meta.json")
        inf_src = os.path.join(os.path.dirname(__file__), "inference.py")
        if os.path.exists(inf_src):
            tar.add(inf_src, arcname="inference.py")

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
