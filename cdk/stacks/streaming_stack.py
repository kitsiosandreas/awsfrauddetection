"""
Streaming Stack
Amazon MSK cluster with IAM authentication + TLS encryption.
A Lambda-backed custom resource creates Kafka topics post-cluster creation.
"""
from aws_cdk import (
    Stack, RemovalPolicy, Duration, CfnOutput,
    aws_ec2 as ec2,
    aws_msk as msk,
    aws_iam as iam,
    aws_logs as logs,
    aws_lambda as _lambda,
    custom_resources as cr,
)
from constructs import Construct


# Topic definitions: name → (partitions, replication_factor)
KAFKA_TOPICS = {
    "trades.raw":              (12, 2),
    "sessions.raw":            (6,  2),
    "registrations.raw":       (3,  2),
    "api.calls":               (12, 2),
    "alerts.fraud":            (6,  2),
    "features.realtime":       (6,  2),
    "dlq.processing-errors":   (3,  2),
}


class StreamingStack(Stack):

    def __init__(self, scope: Construct, construct_id: str,
                 vpc: ec2.Vpc, msk_sg: ec2.SecurityGroup,
                 rds_endpoint: str, rds_credentials_arn: str,
                 config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ── MSK Configuration ────────────────────────────────────────────────
        server_props = "\n".join([
            "auto.create.topics.enable=false",
            "default.replication.factor=2",
            "min.insync.replicas=1",
            "num.partitions=6",
            "num.io.threads=8",
            "num.network.threads=5",
            "log.retention.hours=24",
            "log.retention.bytes=10737418240",
            "message.max.bytes=1048576",
            "compression.type=lz4",
            "offsets.topic.replication.factor=2",
        ])

        msk_config = msk.CfnConfiguration(
            self, "MskConfig",
            name="fraud-demo-msk-config",
            server_properties=server_props,
        )

        # ── CloudWatch Log Group for MSK ──────────────────────────────────────
        msk_log_group = logs.LogGroup(
            self, "MskLogGroup",
            log_group_name="/aws/msk/fraud-demo",
            removal_policy=RemovalPolicy.DESTROY,
            retention=logs.RetentionDays.ONE_WEEK,
        )

        # ── MSK Cluster ──────────────────────────────────────────────────────
        private_subnet_ids = [s.subnet_id for s in vpc.private_subnets[:3]]

        self.msk_cluster = msk.CfnCluster(
            self, "MskCluster",
            cluster_name="fraud-demo-cluster",
            kafka_version="3.6.0",
            number_of_broker_nodes=3,
            broker_node_group_info=msk.CfnCluster.BrokerNodeGroupInfoProperty(
                instance_type="kafka.m5.large",
                client_subnets=private_subnet_ids,
                security_groups=[msk_sg.security_group_id],
                storage_info=msk.CfnCluster.StorageInfoProperty(
                    ebs_storage_info=msk.CfnCluster.EBSStorageInfoProperty(volume_size=100)
                ),
            ),
            client_authentication=msk.CfnCluster.ClientAuthenticationProperty(
                sasl=msk.CfnCluster.SaslProperty(
                    iam=msk.CfnCluster.IamProperty(enabled=True)
                ),
                unauthenticated=msk.CfnCluster.UnauthenticatedProperty(enabled=False),
            ),
            encryption_info=msk.CfnCluster.EncryptionInfoProperty(
                encryption_in_transit=msk.CfnCluster.EncryptionInTransitProperty(
                    client_broker="TLS", in_cluster=True
                )
            ),
            configuration_info=msk.CfnCluster.ConfigurationInfoProperty(
                arn=msk_config.attr_arn, revision=1
            ),
            enhanced_monitoring="PER_TOPIC_PER_BROKER",
            logging_info=msk.CfnCluster.LoggingInfoProperty(
                broker_logs=msk.CfnCluster.BrokerLogsProperty(
                    cloud_watch_logs=msk.CfnCluster.CloudWatchLogsProperty(
                        enabled=True,
                        log_group=msk_log_group.log_group_name,
                    )
                )
            ),
        )

        # ── Lambda — Kafka Topic Initializer (custom resource) ───────────────
        topic_init_role = iam.Role(
            self, "TopicInitRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaVPCAccessExecutionRole"
                ),
            ],
        )
        topic_init_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "kafka-cluster:Connect",
                "kafka-cluster:DescribeCluster",
                "kafka-cluster:CreateTopic",
                "kafka-cluster:DescribeTopic",
                "kafka-cluster:AlterTopic",
                "kafka:GetBootstrapBrokers",
                "kafka:ListClusters",
                "kafka:DescribeCluster",
            ],
            resources=["*"],
        ))

        topic_init_fn = _lambda.Function(
            self, "TopicInitializer",
            function_name="fraud-demo-topic-initializer",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="handler.handler",
            code=_lambda.Code.from_asset("../lambda_functions/topic_initializer"),
            role=topic_init_role,
            timeout=Duration.minutes(5),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            security_groups=[msk_sg],
            environment={
                "MSK_CLUSTER_ARN": self.msk_cluster.attr_arn,
                "TOPICS_CONFIG": str(KAFKA_TOPICS),
            },
        )

        # Invoke the topic initializer as a CloudFormation custom resource
        topic_provider = cr.Provider(
            self, "TopicInitProvider",
            on_event_handler=topic_init_fn,
        )
        from aws_cdk import CustomResource
        CustomResource(
            self, "KafkaTopicInit",
            service_token=topic_provider.service_token,
            properties={"ClusterArn": self.msk_cluster.attr_arn},
        )

        # ── Outputs ──────────────────────────────────────────────────────────
        CfnOutput(self, "MskClusterArn",
                  value=self.msk_cluster.attr_arn,
                  export_name="FraudDemo-MskClusterArn")
        CfnOutput(self, "MskBootstrapHint",
                  value=f"aws kafka get-bootstrap-brokers --cluster-arn {self.msk_cluster.attr_arn}",
                  export_name="FraudDemo-MskBootstrapHint")
