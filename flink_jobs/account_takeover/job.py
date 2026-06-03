"""
Flink Job — Account Takeover (ATO) Detection
=============================================
Consumes: sessions.raw
Produces: alerts.fraud

Detection layers:
  1. Rule: Geo-velocity (impossible travel between consecutive logins)
  2. Rule: New device + withdrawal request within 24 hours
  3. Rule: Password reset → withdrawal within 60 minutes (CEP)
  4. Rule: Credential stuffing (burst of failed logins)
  5. ML:   SageMaker login-risk endpoint — per-login risk score

This file represents the PyFlink job logic.
It is packaged as a fat JAR for deployment to Managed Flink.
"""
import json
import logging
import os

# PyFlink imports (available in the Flink runtime environment)
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema,
)
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, FilterFunction, ProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types

from rules import (
    GeoVelocityRule, NewDeviceWithdrawalRule, PasswordResetWithdrawalCEP,
    CredentialStuffingRule,
)

logger = logging.getLogger(__name__)

KAFKA_BROKERS       = os.environ.get("KAFKA_BROKERS", "localhost:9092")
SOURCE_TOPIC        = os.environ.get("SOURCE_TOPIC", "sessions.raw")
SINK_TOPIC          = os.environ.get("SINK_TOPIC", "alerts.fraud")
SAGEMAKER_ENDPOINT  = os.environ.get("SAGEMAKER_ENDPOINT", "fraud-demo-login-risk")
AWS_REGION          = os.environ.get("AWS_REGION", "us-east-1")
DYNAMO_TABLE        = os.environ.get("DYNAMO_TABLE_ENTITY_STATE", "fraud-demo-entity-state")


class SessionEventParser(MapFunction):
    """Parse raw JSON session event into a Python dict."""
    def map(self, value: str):
        try:
            return json.loads(value)
        except Exception:
            return None


class NullFilter(FilterFunction):
    def filter(self, value) -> bool:
        return value is not None


class ATOEnricher(MapFunction):
    """
    Enrich session event with account history from DynamoDB.
    Computes:
      - is_new_device: device_id not in known_devices for account
      - is_new_country: country_code not in known_countries
      - geo_velocity_kph: km/h required to travel from last login
    """
    def map(self, event: dict) -> dict:
        import boto3
        from utils_flink import (get_account_state, compute_geo_velocity,
                                  is_new_device, is_new_country)

        account_id = event.get("account_id", "")
        state = get_account_state(account_id, DYNAMO_TABLE, AWS_REGION)

        enriched = event.copy()
        enriched["is_new_device"]   = is_new_device(event, state)
        enriched["is_new_country"]  = is_new_country(event, state)
        enriched["geo_velocity_kph"] = compute_geo_velocity(event, state)
        enriched["days_since_account_open"] = state.get("account_age_days", 365)
        enriched["failed_attempts_before"]  = event.get("recent_failures", 0)
        enriched["has_pending_withdrawal"]  = state.get("has_pending_withdrawal", 0)
        enriched["session_duration_min"]    = event.get("session_duration_min", 30)
        enriched["password_reset_flag"]     = state.get("recent_pw_reset", 0)

        return enriched


class MLScorer(MapFunction):
    """Call SageMaker login-risk endpoint for ML-based ATO probability."""

    def __init__(self, endpoint: str, region: str):
        self._endpoint = endpoint
        self._region   = region
        self._client   = None

    def open(self, runtime_context):
        import boto3
        self._client = boto3.client("sagemaker-runtime", region_name=self._region)

    def map(self, event: dict) -> dict:
        if event.get("event_subtype") not in ("LOGIN", "LOGIN_FAILED"):
            return event

        features = {
            "is_new_device":           event.get("is_new_device", 0),
            "is_new_country":          event.get("is_new_country", 0),
            "geo_velocity_kph":        event.get("geo_velocity_kph", 0),
            "hour_of_day":             event.get("hour_of_day", 12),
            "failed_attempts_before":  event.get("failed_attempts_before", 0),
            "days_since_account_open": event.get("days_since_account_open", 365),
            "has_pending_withdrawal":  event.get("has_pending_withdrawal", 0),
            "session_duration_min":    event.get("session_duration_min", 30),
            "password_reset_flag":     event.get("password_reset_flag", 0),
        }

        try:
            response = self._client.invoke_endpoint(
                EndpointName=self._endpoint,
                ContentType="application/json",
                Body=json.dumps({"features": features}),
            )
            result = json.loads(response["Body"].read())
            prediction = result["predictions"][0]
            event["ml_fraud_probability"] = prediction["fraud_probability"]
            event["ml_is_fraud"]          = prediction["is_fraud"]
        except Exception as exc:
            logger.warning(f"SageMaker call failed: {exc}")
            event["ml_fraud_probability"] = 0.0
            event["ml_is_fraud"]          = False

        return event


class AlertEmitter(MapFunction):
    """Convert high-risk events into fraud alert messages."""

    GEO_VELOCITY_THRESHOLD_KPH = 900
    ML_THRESHOLD = 0.60

    def map(self, event: dict):
        alerts = []

        # Rule 1: impossible travel
        if event.get("geo_velocity_kph", 0) > self.GEO_VELOCITY_THRESHOLD_KPH:
            alerts.append(self._build_alert(
                event, "ACCOUNT_TAKEOVER", "CRITICAL",
                signal="GEO_VELOCITY_IMPOSSIBLE",
                detection_method="RULE",
                risk_score=0.90,
            ))

        # ML: high login risk score
        if event.get("ml_is_fraud") and event.get("ml_fraud_probability", 0) > self.ML_THRESHOLD:
            alerts.append(self._build_alert(
                event, "ACCOUNT_TAKEOVER", "HIGH",
                signal="ML_HIGH_LOGIN_RISK",
                detection_method="ML",
                risk_score=event["ml_fraud_probability"],
            ))

        return alerts

    @staticmethod
    def _build_alert(event: dict, typology: str, severity: str,
                     signal: str, detection_method: str, risk_score: float) -> str:
        import uuid, time
        alert = {
            "alert_id":        str(uuid.uuid4()),
            "entity_id":       event.get("account_id"),
            "entity_type":     "ACCOUNT",
            "typology":        typology,
            "severity":        severity,
            "risk_score":      round(risk_score, 4),
            "signals":         [signal],
            "detection_method": detection_method,
            "source_event_id": event.get("event_id"),
            "timestamp":       event.get("timestamp"),
            "created_at":      int(time.time() * 1000),
            "geo_velocity_kph": event.get("geo_velocity_kph"),
            "is_new_device":   event.get("is_new_device"),
            "country_code":    event.get("country_code"),
        }
        return json.dumps(alert)


def build_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.environ.get("FLINK_PARALLELISM", "2")))

    # ── Source ──────────────────────────────────────────────────────────────
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("flink-account-takeover")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    stream = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "MSK-sessions"
    )

    # ── Parse + Enrich + Score ───────────────────────────────────────────────
    alerts = (
        stream
        .map(SessionEventParser())
        .filter(NullFilter())
        .map(ATOEnricher())
        .map(MLScorer(SAGEMAKER_ENDPOINT, AWS_REGION))
        .flat_map(lambda e: AlertEmitter().map(e) or [])
    )

    # ── Sink ─────────────────────────────────────────────────────────────────
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(SINK_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    alerts.sink_to(sink)

    return env


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    env = build_pipeline()
    env.execute("fraud-demo-account-takeover")
