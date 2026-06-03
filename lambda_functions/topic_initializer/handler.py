"""
Lambda Custom Resource — Kafka Topic Initializer
Creates MSK topics after cluster becomes available.
"""
import ast
import json
import logging
import os
import time

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

MSK_CLUSTER_ARN = os.environ["MSK_CLUSTER_ARN"]
TOPICS_CONFIG_STR = os.environ.get("TOPICS_CONFIG", "{}")


def _get_bootstrap_servers() -> str:
    kafka = boto3.client("kafka")
    response = kafka.get_bootstrap_brokers(ClusterArn=MSK_CLUSTER_ARN)
    return response.get("BootstrapBrokerStringSaslIam", "")


def _create_topics(bootstrap_servers: str, topics: dict) -> dict:
    """Create topics using kafka-python admin client."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers.split(","),
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
    )

    new_topics = [
        NewTopic(
            name=name,
            num_partitions=cfg[0],
            replication_factor=cfg[1],
        )
        for name, cfg in topics.items()
    ]

    results = {"created": [], "existing": [], "failed": []}
    try:
        admin.create_topics(new_topics, validate_only=False)
        results["created"] = list(topics.keys())
        logger.info(f"Created topics: {topics.keys()}")
    except TopicAlreadyExistsError:
        results["existing"] = list(topics.keys())
        logger.info("Topics already exist — skipping")
    except Exception as exc:
        logger.error(f"Topic creation error: {exc}")
        results["failed"] = list(topics.keys())
    finally:
        admin.close()

    return results


def handler(event, context):
    """CloudFormation Custom Resource handler."""
    import cfnresponse  # available in Lambda runtime

    request_type = event.get("RequestType", "Create")
    logger.info(f"Request type: {request_type}")

    if request_type == "Delete":
        cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
        return

    try:
        topics = ast.literal_eval(TOPICS_CONFIG_STR)
        # Wait for MSK brokers to be ready
        for attempt in range(10):
            try:
                bootstrap = _get_bootstrap_servers()
                if bootstrap:
                    break
            except Exception:
                pass
            logger.info(f"Waiting for MSK brokers (attempt {attempt + 1}/10)...")
            time.sleep(30)

        if not bootstrap:
            raise RuntimeError("Could not resolve MSK bootstrap servers after 5 minutes")

        results = _create_topics(bootstrap, topics)
        cfnresponse.send(event, context, cfnresponse.SUCCESS, results)

    except Exception as exc:
        logger.error(f"Topic init failed: {exc}")
        cfnresponse.send(event, context, cfnresponse.FAILED, {"Error": str(exc)})
