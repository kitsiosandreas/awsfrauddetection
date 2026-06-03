"""
Registration Anomaly Model — SageMaker Inference Script
Uses reconstruction error from autoencoder to score anomalies.
"""
import json
import os
import pickle
import numpy as np

_artifact = None
_features = [
    "ip_registration_count",
    "device_fp_count",
    "kyc_similarity_score",
    "registration_hour",
    "form_fill_time_sec",
    "email_domain_risk",
    "api_rate_per_min",
    "order_cancel_ratio",
]


def model_fn(model_dir: str):
    global _artifact
    with open(os.path.join(model_dir, "model.pkl"), "rb") as f:
        _artifact = pickle.load(f)
    return _artifact


def input_fn(request_body: str, content_type: str = "application/json"):
    data = json.loads(request_body)
    if "instances" in data:
        return np.array(data["instances"], dtype=float)
    elif "features" in data:
        feats = data["features"]
        row = [feats.get(f, 0.0) for f in _features]
        return np.array([row], dtype=float)
    raise ValueError("Expected 'instances' or 'features' key")


def predict_fn(input_data: np.ndarray, artifact):
    scaler      = artifact["scaler"]
    autoencoder = artifact["autoencoder"]
    threshold   = artifact["threshold"]

    X_scaled    = scaler.transform(input_data)
    recon       = autoencoder.predict(X_scaled)
    errors      = np.mean((X_scaled - recon) ** 2, axis=1)
    # Normalise error to probability-like score
    proba = np.clip(errors / (threshold * 3), 0, 1)
    return list(zip(errors.tolist(), proba.tolist(), (errors > threshold).tolist()))


def output_fn(prediction, accept: str = "application/json"):
    results = [
        {
            "reconstruction_error": round(err, 6),
            "fraud_probability":    round(prob, 4),
            "is_fraud":             bool(is_fraud),
        }
        for err, prob, is_fraud in prediction
    ]
    return json.dumps({"predictions": results}), "application/json"
