#!/usr/bin/env python3
"""
Fraud Detection Demo — CDK App Entry Point

Deploys all stacks in dependency order:
  1. NetworkingStack     — VPC, subnets, security groups
  2. DataStorageStack    — RDS, S3, DynamoDB
  3. StreamingStack      — MSK cluster
  4. GraphStack          — Neptune cluster
  5. SearchStack         — OpenSearch domain
  6. MLStack             — SageMaker feature store & endpoints
  7. ProcessingStack     — Managed Flink applications
  8. AlertingStack       — EventBridge, SNS, Step Functions
  9. ComputeStack        — ECS data generator, Lambda functions
"""
import aws_cdk as cdk
from stacks.networking_stack import NetworkingStack
from stacks.data_storage_stack import DataStorageStack
from stacks.streaming_stack import StreamingStack
from stacks.graph_stack import GraphStack
from stacks.search_stack import SearchStack
from stacks.ml_stack import MLStack
from stacks.processing_stack import ProcessingStack
from stacks.alerting_stack import AlertingStack
from stacks.compute_stack import ComputeStack

app = cdk.App()

# ---------------------------------------------------------------------------
# Shared configuration — override via CDK context or environment variables
# ---------------------------------------------------------------------------
config = {
    "environment": app.node.try_get_context("environment") or "demo",
    "alert_email": app.node.try_get_context("alertEmail") or "fraud-demo-alerts@example.com",
    "rds_instance_class": app.node.try_get_context("rdsInstanceClass") or "db.r5.large",
    "msk_instance_type": app.node.try_get_context("mskInstanceType") or "kafka.m5.large",
    "opensearch_instance_type": app.node.try_get_context("opensearchInstanceType") or "m5.large.search",
    "sagemaker_instance_type": app.node.try_get_context("sagemakerInstanceType") or "ml.m5.large",
    "generator_tps": int(app.node.try_get_context("generatorTps") or "200"),
    "generator_accounts": int(app.node.try_get_context("generatorAccounts") or "1000"),
    "generator_scenario": app.node.try_get_context("generatorScenario") or "mixed",
    "fraud_injection_rate": float(app.node.try_get_context("fraudInjectionRate") or "0.15"),
}

env = cdk.Environment(
    account=app.node.try_get_context("account"),
    region=app.node.try_get_context("region") or "us-east-1",
)

prefix = "FraudDemo"

# ---------------------------------------------------------------------------
# Stack 1: Networking
# ---------------------------------------------------------------------------
networking = NetworkingStack(app, f"{prefix}-Networking", config=config, env=env)

# ---------------------------------------------------------------------------
# Stack 2: Data Storage (RDS + S3 + DynamoDB)
# ---------------------------------------------------------------------------
storage = DataStorageStack(
    app, f"{prefix}-Storage",
    vpc=networking.vpc,
    rds_sg=networking.rds_sg,
    config=config,
    env=env,
)
storage.add_dependency(networking)

# ---------------------------------------------------------------------------
# Stack 3: Streaming (MSK)
# ---------------------------------------------------------------------------
streaming = StreamingStack(
    app, f"{prefix}-Streaming",
    vpc=networking.vpc,
    msk_sg=networking.msk_sg,
    rds_endpoint=storage.rds_instance.db_instance_endpoint_address,
    rds_credentials_arn=storage.db_credentials.secret_arn,
    config=config,
    env=env,
)
streaming.add_dependency(storage)

# ---------------------------------------------------------------------------
# Stack 4: Graph (Neptune)
# ---------------------------------------------------------------------------
graph = GraphStack(
    app, f"{prefix}-Graph",
    vpc=networking.vpc,
    neptune_sg=networking.neptune_sg,
    config=config,
    env=env,
)
graph.add_dependency(networking)

# ---------------------------------------------------------------------------
# Stack 5: Search (OpenSearch)
# ---------------------------------------------------------------------------
search = SearchStack(
    app, f"{prefix}-Search",
    vpc=networking.vpc,
    opensearch_sg=networking.opensearch_sg,
    config=config,
    env=env,
)
search.add_dependency(networking)

# ---------------------------------------------------------------------------
# Stack 6: ML (SageMaker)
# ---------------------------------------------------------------------------
ml = MLStack(
    app, f"{prefix}-ML",
    vpc=networking.vpc,
    ml_artifacts_bucket=storage.ml_artifacts_bucket,
    config=config,
    env=env,
)
ml.add_dependency(storage)

# ---------------------------------------------------------------------------
# Stack 7: Processing (Flink)
# ---------------------------------------------------------------------------
processing = ProcessingStack(
    app, f"{prefix}-Processing",
    vpc=networking.vpc,
    flink_sg=networking.flink_sg,
    msk_cluster_arn=streaming.msk_cluster.attr_arn,
    flink_artifacts_bucket=storage.flink_artifacts_bucket,
    neptune_endpoint=graph.neptune_cluster.cluster_endpoint.socket_address,
    opensearch_endpoint=search.opensearch_domain.domain_endpoint,
    config=config,
    env=env,
)
processing.add_dependency(streaming)
processing.add_dependency(ml)
processing.add_dependency(graph)
processing.add_dependency(search)

# ---------------------------------------------------------------------------
# Stack 8: Alerting (EventBridge + SNS + Step Functions)
# ---------------------------------------------------------------------------
alerting = AlertingStack(
    app, f"{prefix}-Alerting",
    vpc=networking.vpc,
    lambda_sg=networking.lambda_sg,
    alerts_table_name=storage.alerts_table.table_name,
    opensearch_endpoint=search.opensearch_domain.domain_endpoint,
    opensearch_domain_arn=search.opensearch_domain.domain_arn,
    event_bus_arn=None,  # Created in this stack
    config=config,
    env=env,
)
alerting.add_dependency(storage)
alerting.add_dependency(search)

# ---------------------------------------------------------------------------
# Stack 9: Compute (ECS data generator + Lambda topic initializer)
# ---------------------------------------------------------------------------
compute = ComputeStack(
    app, f"{prefix}-Compute",
    vpc=networking.vpc,
    ecs_sg=networking.ecs_sg,
    lambda_sg=networking.lambda_sg,
    msk_cluster_arn=streaming.msk_cluster.attr_arn,
    rds_secret_arn=storage.db_credentials.secret_arn,
    rds_endpoint=storage.rds_instance.db_instance_endpoint_address,
    raw_data_bucket=storage.raw_data_bucket,
    enrichment_bucket=storage.enrichment_bucket,
    config=config,
    env=env,
)
compute.add_dependency(streaming)
compute.add_dependency(storage)

app.synth()
