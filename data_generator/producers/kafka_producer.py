"""
Kafka producer wrapper with MSK IAM auth support.
"""
import json
import logging
import os
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


def _get_bootstrap_servers(msk_cluster_arn: str, region: str) -> str:
    """Resolve MSK bootstrap servers via boto3 if not supplied directly."""
    import boto3
    client = boto3.client("kafka", region_name=region)
    response = client.get_bootstrap_brokers(ClusterArn=msk_cluster_arn)
    return response.get("BootstrapBrokerStringSaslIam", "")


def build_producer(
    bootstrap_servers: str,
    use_iam_auth: bool = True,
    region: str = "us-east-1",
    msk_cluster_arn: str = "",
) -> KafkaProducer:
    """
    Build a KafkaProducer with MSK IAM auth (production) or plain (local).
    """
    if not bootstrap_servers and msk_cluster_arn:
        bootstrap_servers = _get_bootstrap_servers(msk_cluster_arn, region)

    servers = [s.strip() for s in bootstrap_servers.split(",")]

    if use_iam_auth:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        def oauth_cb(oauth_config):
            auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
            return auth_token, expiry_ms / 1000

        producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
            sasl_oauth_token_provider=type(
                "OAuthProvider", (),
                {"token": lambda self: oauth_cb(None)}
            )(),
            acks="all",
            retries=3,
            linger_ms=5,
            batch_size=65536,
        )
    else:
        # Local development — no auth
        producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks=1,
            linger_ms=5,
        )

    return producer


def send_event(
    producer: KafkaProducer,
    topic: str,
    event: dict,
    key: Optional[str] = None,
    on_error=None,
) -> None:
    """Non-blocking send with error callback."""
    def _on_send_error(exc):
        logger.error(f"Failed to send to {topic}: {exc}")
        if on_error:
            on_error(exc)

    producer.send(topic, key=key, value=event).add_errback(_on_send_error)
