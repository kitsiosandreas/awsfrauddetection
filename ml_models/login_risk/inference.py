"""
Login Risk Model — SageMaker Inference Script

Input (JSON):  {"features": {"is_new_device": 1, "geo_velocity_kph": 5400, ...}}
               OR {"instances": [[1, 1, 5400, ...]]}
Output (JSON): {"predictions": [{"fraud_probability": 0.94, "is_fraud": true}]}
"""
import json
import os
import pickle
import numpy as np

_model = None
_features = [
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


def model_fn(model_dir: str):
    global _model
    with open(os.path.join(model_dir, "model.pkl"), "rb") as f:
        _model = pickle.load(f)
    return _model


def input_fn(request_body: str, content_type: str = "application/json"):
    data = json.loads(request_body)
    if "instances" in data:
        return np.array(data["instances"], dtype=float)
    elif "features" in data:
        feats = data["features"]
        row = [feats.get(f, 0.0) for f in _features]
        return np.array([row], dtype=float)
    raise ValueError("Expected 'instances' or 'features' key")


def predict_fn(input_data: np.ndarray, model):
    proba = model.predict_proba(input_data)[:, 1]  # P(fraud)
    return proba.tolist()


def output_fn(prediction, accept: str = "application/json"):
    threshold = 0.60
    results = [
        {
            "fraud_probability": round(p, 4),
            "is_fraud": bool(p >= threshold),
            "threshold": threshold,
        }
        for p in prediction
    ]
    return json.dumps({"predictions": results}), "application/json"
