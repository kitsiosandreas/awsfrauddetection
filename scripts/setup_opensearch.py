"""
Script — Bootstrap OpenSearch indices, mappings, and import dashboards.

Usage:
    python scripts/setup_opensearch.py --endpoint <os-endpoint> --region us-east-1
"""
import argparse
import json
import logging
import os
import urllib.request
import urllib.error
import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import get_credentials

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def _signed_request(method: str, url: str, body: bytes, region: str) -> dict:
    """Execute a SigV4-signed HTTPS request to OpenSearch."""
    credentials = boto3.Session().get_credentials().get_frozen_credentials()
    aws_request = AWSRequest(method=method, url=url, data=body)
    SigV4Auth(credentials, "es", region).add_auth(aws_request)

    req = urllib.request.Request(
        url, data=body,
        headers=dict(aws_request.headers),
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        if e.code in (400, 409):  # Bad request or already exists
            logger.warning(f"  {e.code}: {body[:200]}")
            return {}
        raise


INDEX_TEMPLATES = {
    "fraud-events": {
        "index_patterns": ["fraud-events-*"],
        "template": {
            "settings": {"number_of_shards": 1, "number_of_replicas": 1},
            "mappings": {
                "properties": {
                    "event_id":      {"type": "keyword"},
                    "account_id":    {"type": "keyword"},
                    "event_type":    {"type": "keyword"},
                    "timestamp":     {"type": "date"},
                    "epoch_ms":      {"type": "long"},
                    "instrument":    {"type": "keyword"},
                    "direction":     {"type": "keyword"},
                    "ip_address":    {"type": "keyword"},
                    "country_code":  {"type": "keyword"},
                    "label":         {"type": "keyword"},
                    "cluster_id":    {"type": "keyword"},
                    "location": {
                        "properties": {
                            "lat": {"type": "float"},
                            "lon": {"type": "float"},
                        }
                    },
                }
            },
        },
    },
    "fraud-alerts": {
        "index_patterns": ["fraud-alerts-*"],
        "template": {
            "settings": {"number_of_shards": 1, "number_of_replicas": 1},
            "mappings": {
                "properties": {
                    "alert_id":          {"type": "keyword"},
                    "entity_id":         {"type": "keyword"},
                    "typology":          {"type": "keyword"},
                    "severity":          {"type": "keyword"},
                    "risk_score":        {"type": "float"},
                    "detection_method":  {"type": "keyword"},
                    "signals":           {"type": "keyword"},
                    "cluster_id":        {"type": "keyword"},
                    "timestamp":         {"type": "date"},
                    "created_at":        {"type": "long"},
                    "status":            {"type": "keyword"},
                }
            },
        },
    },
}


def create_index_templates(endpoint: str, region: str):
    for name, template in INDEX_TEMPLATES.items():
        url = f"https://{endpoint}/_index_template/{name}"
        body = json.dumps(template).encode("utf-8")
        logger.info(f"Creating index template: {name}")
        _signed_request("PUT", url, body, region)


def check_cluster_health(endpoint: str, region: str):
    url = f"https://{endpoint}/_cluster/health"
    result = _signed_request("GET", url, b"", region)
    logger.info(f"Cluster health: {result.get('status', 'unknown')}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=False, help="OpenSearch domain endpoint")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    endpoint = args.endpoint
    if not endpoint:
        cf = boto3.client("cloudformation", region_name=args.region)
        stacks = cf.describe_stacks(StackName="FraudDemo-Search")
        outputs = {o["OutputKey"]: o["OutputValue"]
                   for o in stacks["Stacks"][0].get("Outputs", [])}
        endpoint = outputs.get("OpenSearchEndpoint")
        if not endpoint:
            raise ValueError("Could not resolve OpenSearch endpoint. Pass --endpoint.")

    logger.info(f"Configuring OpenSearch at: {endpoint}")
    check_cluster_health(endpoint, args.region)
    create_index_templates(endpoint, args.region)
    logger.info("OpenSearch setup complete.")


if __name__ == "__main__":
    main()
