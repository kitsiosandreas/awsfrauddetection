"""
Coordinated Trading Model — SageMaker Inference Script
Called by the SageMaker sklearn container via the inference.py entry point.

Input (JSON):  {"instances": [[f1, f2, ..., f7], ...]}
Output (JSON): {"predictions": [{"score": -0.12, "is_fraud": true}, ...]}
"""
import json
import os
import pickle
import numpy as np

_model = None
_features = [
    "trade_timing_entropy",
    "instrument_concentration",
    "win_rate_7d",
    "avg_hold_duration_min",
    "pnl_volatility_7d",
    "correlated_accounts_cnt",
    "trade_count_24h",
]


def model_fn(model_dir: str):
    global _model
    model_path = os.path.join(model_dir, "model.pkl")
    with open(model_path, "rb") as f:
        _model = pickle.load(f)
    return _model


def input_fn(request_body: str, content_type: str = "application/json"):
    """Parse incoming request."""
    if content_type == "application/json":
        data = json.loads(request_body)
        # Accept {"instances": [[...]]} or {"features": {...}}
        if "instances" in data:
            return np.array(data["instances"], dtype=float)
        elif "features" in data:
            feats = data["features"]
            row = [feats.get(f, 0.0) for f in _features]
            return np.array([row], dtype=float)
    raise ValueError(f"Unsupported content type: {content_type}")


def predict_fn(input_data: np.ndarray, model):
    """Run inference and return anomaly scores."""
    scores = model.decision_function(input_data)
    predictions = model.predict(input_data)
    # Normalise decision score to [0, 1] probability-like range
    # Isolation Forest returns negative scores for anomalies
    normalised = 1 / (1 + np.exp(scores * 5))  # sigmoid transformation
    return list(zip(scores.tolist(), normalised.tolist(), predictions.tolist()))


def output_fn(prediction, accept: str = "application/json"):
    """Format output."""
    results = [
        {
            "raw_score":  raw,
            "fraud_probability": round(prob, 4),
            "is_fraud":   bool(pred == -1),
            "threshold":  0.6,
        }
        for raw, prob, pred in prediction
    ]
    return json.dumps({"predictions": results}), "application/json"
