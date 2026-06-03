"""
Lambda — Alert Processor
Receives fraud alert events from EventBridge,
writes to DynamoDB and OpenSearch, fans out high-severity to SNS.
"""
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
import urllib.request

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ALERTS_TABLE       = os.environ["ALERTS_TABLE"]
OPENSEARCH_ENDPOINT= os.environ["OPENSEARCH_ENDPOINT"]
CRITICAL_TOPIC_ARN = os.environ["CRITICAL_TOPIC_ARN"]
STANDARD_TOPIC_ARN = os.environ["STANDARD_TOPIC_ARN"]
EVENT_BUS_NAME     = os.environ["EVENT_BUS_NAME"]
AWS_REGION         = os.environ.get("AWS_REGION", "us-east-1")

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
sns      = boto3.client("sns",      region_name=AWS_REGION)
table    = dynamodb.Table(ALERTS_TABLE)

SEVERITY_ORDER = {"CRITICAL": 3, "HIGH": 2, "MEDIUM": 1, "LOW": 0}


def _index_opensearch(alert: dict) -> None:
    """Index the alert in OpenSearch via SigV4-signed HTTPS request."""
    try:
        index = f"fraud-alerts-{datetime.now(timezone.utc).strftime('%Y.%m.%d')}"
        url = f"https://{OPENSEARCH_ENDPOINT}/{index}/_doc/{alert['alert_id']}"
        body = json.dumps(alert).encode("utf-8")

        credentials = boto3.Session().get_credentials().get_frozen_credentials()
        request = AWSRequest(method="PUT", url=url, data=body)
        SigV4Auth(credentials, "es", AWS_REGION).add_auth(request)

        req = urllib.request.Request(
            url, data=body,
            headers=dict(request.headers),
            method="PUT",
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as exc:
        logger.warning(f"OpenSearch indexing failed (non-fatal): {exc}")


def _notify_sns(alert: dict) -> None:
    """Publish alert to SNS (critical or standard topic)."""
    severity = alert.get("severity", "MEDIUM")
    topic_arn = CRITICAL_TOPIC_ARN if severity == "CRITICAL" else STANDARD_TOPIC_ARN

    subject = f"[{severity}] Fraud Alert: {alert.get('typology', 'UNKNOWN')} — Account {alert.get('entity_id', 'UNKNOWN')}"
    message = (
        f"Fraud Alert Detected\n\n"
        f"  Alert ID:   {alert.get('alert_id')}\n"
        f"  Typology:   {alert.get('typology')}\n"
        f"  Severity:   {severity}\n"
        f"  Account:    {alert.get('entity_id')}\n"
        f"  Risk Score: {alert.get('risk_score', 'N/A')}\n"
        f"  Signals:    {json.dumps(alert.get('signals', []))}\n"
        f"  Timestamp:  {alert.get('timestamp')}\n\n"
        f"  Detection:  {alert.get('detection_method', 'UNKNOWN')}"
    )

    sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=message)


def _persist_dynamodb(alert: dict) -> None:
    """Write alert to DynamoDB alerts table."""
    ttl = int(time.time()) + (30 * 24 * 3600)  # 30 days
    table.put_item(Item={
        **alert,
        "ttl": ttl,
        "status": "NEW",
    })


def handler(event, context):
    """EventBridge event → DynamoDB + OpenSearch + SNS fan-out."""
    logger.info(f"Received event: {json.dumps(event)[:500]}")

    try:
        detail = event.get("detail", event)  # EventBridge wraps payload in 'detail'

        alert = {
            "alert_id":         detail.get("alert_id", str(uuid.uuid4())),
            "entity_id":        detail.get("entity_id", "unknown"),
            "entity_type":      detail.get("entity_type", "ACCOUNT"),
            "typology":         detail.get("typology", "UNKNOWN"),
            "severity":         detail.get("severity", "MEDIUM"),
            "risk_score":       detail.get("risk_score", 0.0),
            "signals":          detail.get("signals", []),
            "detection_method": detail.get("detection_method", "UNKNOWN"),
            "source_event_id":  detail.get("source_event_id"),
            "cluster_id":       detail.get("cluster_id"),
            "timestamp":        detail.get("timestamp", datetime.now(timezone.utc).isoformat()),
            "created_at":       int(time.time() * 1000),
            "raw_event":        json.dumps(detail)[:5000],
        }

        _persist_dynamodb(alert)
        _index_opensearch(alert)

        if SEVERITY_ORDER.get(alert["severity"], 0) >= SEVERITY_ORDER.get("HIGH", 2):
            _notify_sns(alert)

        logger.info(f"Alert {alert['alert_id']} processed: {alert['severity']} | {alert['typology']}")
        return {"statusCode": 200, "alertId": alert["alert_id"]}

    except Exception as exc:
        logger.error(f"Alert processing failed: {exc}", exc_info=True)
        raise  # Let Lambda retry


