
from aws_cdk import (
    Stack, RemovalPolicy, CfnOutput,
    aws_ec2 as ec2,
    aws_msk as msk,
    aws_msk_alpha as msk_alpha,  # For L2 constructs (if available)
    aws_iam as iam,
    aws_logs as logs,
)
from constructs import Construct


class StreamingStack(Stack):
    """Amazon MSK cluster and MSK Connect for Fraud Detection Demo."""

    def __init__(self, scope: Construct, construct_id: str,
                 vpc: ec2.Vpc, msk_sg: ec2.SecurityGroup,
                 rds_endpoint: str, rds_credentials_arn: str,
                 config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ===== MSK CLUSTER CONFIGURATION =====
        self.msk_config = msk.CfnConfiguration(
            self, "MskConfiguration",
            name="fraud-demo-msk-config",
            server_properties="
".join([
                "auto.create.topics.enable=false",
                "default.replication.factor=2",
                "min.insync.replicas=1",
                "num.partitions=6",
                "num.io.threads=8",
                "num.network.threads=5",
                "log.retention.hours=24",       # 24h retention for demo
                "log.retention.bytes=10737418240",  # 10GB per partition
                "message.max.bytes=1048576",    # 1MB max message
                "compression.type=lz4",
            ]),
        )

        # ===== MSK CLUSTER =====
        # Using CfnCluster for full configuration control
        private_subnets = vpc.select_subnets(
            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
        ).subnet_ids[:3]  # MSK requires exactly 2 or 3 subnets

        self.msk_cluster = msk.CfnCluster(
            self, "FraudDemoMskCluster",
            cluster_name="fraud-detection-demo",
            kafka_version="3.6.0",
            number_of_broker_nodes=3,
            broker_node_group_info=msk.CfnCluster.BrokerNodeGroupInfoProperty(
                instance_type="kafka.m5.large",
                client_subnets=private_subnets,
                security_groups=[msk_sg.security_group_id],
                storage_info=msk.CfnCluster.StorageInfoProperty(
                    ebs_storage_info=msk.CfnCluster.EBSStorageInfoProperty(
                        volume_size=100,  # 100 GB per broker
                    )
                ),
            ),
            client_authentication=msk.CfnCluster.ClientAuthenticationProperty(
                sasl=msk.CfnCluster.SaslProperty(
                    iam=msk.CfnCluster.IamProperty(enabled=True),
                ),
                unauthenticated=msk.CfnCluster.UnauthenticatedProperty(
                    enabled=False
                ),
            ),
            encryption_info=msk.CfnCluster.EncryptionInfoProperty(
                encryption_in_transit=msk.CfnCluster.EncryptionInTransitProperty(
                    client_broker="TLS",
                    in_cluster=True,
                ),
            ),
            configuration_info=msk.CfnCluster.ConfigurationInfoProperty(
                arn=self.msk_config.attr_arn,
                revision=1,
            ),
            enhanced_monitoring="PER_TOPIC_PER_BROKER",
            logging_info=msk.CfnCluster.LoggingInfoProperty(
                broker_logs=msk.CfnCluster.BrokerLogsProperty(
                    cloud_watch_logs=msk.CfnCluster.CloudWatchLogsProperty(
                        enabled=True,
                        log_group="/aws/msk/fraud-detection-demo",
                    ),
                ),
            ),
        )

        # ===== MSK CONNECT — DEBEZIUM CDC CONNECTOR =====
        # IAM Role for MSK Connect
        msk_connect_role = iam.Role(
            self, "MskConnectRole",
            role_name="fraud-demo-msk-connect-role",
            assumed_by=iam.ServicePrincipal("kafkaconnect.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonMSKFullAccess"
                ),
            ],
        )

        # Add permissions for RDS access, S3, Secrets Manager
        msk_connect_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "secretsmanager:GetSecretValue",
                "rds:DescribeDBInstances",
            ],
            resources=["*"],
        ))

        # Log group for MSK Connect
        connect_log_group = logs.LogGroup(
            self, "MskConnectLogGroup",
            log_group_name="/aws/msk-connect/fraud-demo-debezium",
            removal_policy=RemovalPolicy.DESTROY,
            retention=logs.RetentionDays.ONE_WEEK,
        )

        # NOTE: MSK Connect connector (CfnConnector) requires:
        # 1. Custom plugin uploaded to S3 (Debezium PostgreSQL connector JAR)
        # 2. Worker configuration
        # These are created via a custom resource or manual step post-deployment
        # See: deployment_scripts/setup_msk_connect.py

        # ===== KAFKA TOPIC DEFINITIONS =====
        # Topics are created via a Lambda custom resource post-cluster creation
        self.topic_config = {
            "trades.raw": {"partitions": 12, "replication": 2},
            "sessions.raw": {"partitions": 6, "replication": 2},
            "registrations.raw": {"partitions": 3, "replication": 2},
            "api.calls": {"partitions": 12, "replication": 2},
            "alerts.fraud": {"partitions": 6, "replication": 2},
            "features.realtime": {"partitions": 6, "replication": 2},
            "dlq.processing-errors": {"partitions": 3, "replication": 2},
        }

        # ===== OUTPUTS =====
        CfnOutput(self, "MskClusterArn", value=self.msk_cluster.attr_arn)
        CfnOutput(self, "MskBootstrapBrokers",
                  value=f"Retrieve via: aws kafka get-bootstrap-brokers --cluster-arn {self.msk_cluster.attr_arn}")

