"""
Processing Stack
Amazon Managed Service for Apache Flink — three detection applications.
"""
from aws_cdk import (
    Stack, RemovalPolicy, CfnOutput,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_kinesisanalyticsv2 as flink,
    aws_s3 as s3,
    aws_logs as logs,
)
from constructs import Construct


def _flink_app(
    stack: Stack,
    logical_id: str,
    app_name: str,
    jar_key: str,
    parallelism: int,
    role_arn: str,
    bucket_arn: str,
    vpc: ec2.Vpc,
    flink_sg: ec2.SecurityGroup,
    env_props: list,
):
    """Helper — create a Managed Flink application."""
    log_group = logs.LogGroup(
        stack, f"{logical_id}LogGroup",
        log_group_name=f"/aws/flink/{app_name}",
        removal_policy=RemovalPolicy.DESTROY,
        retention=logs.RetentionDays.ONE_WEEK,
    )
    log_stream = logs.LogStream(
        stack, f"{logical_id}LogStream",
        log_group=log_group,
        log_stream_name="flink-app",
        removal_policy=RemovalPolicy.DESTROY,
    )

    return flink.CfnApplication(
        stack, logical_id,
        application_name=app_name,
        runtime_environment="FLINK-1_18",
        service_execution_role=role_arn,
        application_configuration=flink.CfnApplication.ApplicationConfigurationProperty(
            application_code_configuration=flink.CfnApplication.ApplicationCodeConfigurationProperty(
                code_content=flink.CfnApplication.CodeContentProperty(
                    s3_content_location=flink.CfnApplication.S3ContentLocationProperty(
                        bucket_arn=bucket_arn,
                        file_key=jar_key,
                    )
                ),
                code_content_type="ZIPFILE",
            ),
            flink_application_configuration=flink.CfnApplication.FlinkApplicationConfigurationProperty(
                parallelism_configuration=flink.CfnApplication.ParallelismConfigurationProperty(
                    configuration_type="CUSTOM",
                    parallelism=parallelism,
                    parallelism_per_kpu=1,
                    auto_scaling_enabled=False,
                ),
                checkpoint_configuration=flink.CfnApplication.CheckpointConfigurationProperty(
                    configuration_type="CUSTOM",
                    checkpointing_enabled=True,
                    checkpoint_interval=60000,
                    min_pause_between_checkpoints=30000,
                ),
                monitoring_configuration=flink.CfnApplication.MonitoringConfigurationProperty(
                    configuration_type="CUSTOM",
                    log_level="INFO",
                    metrics_level="APPLICATION",
                ),
            ),
            environment_properties=flink.CfnApplication.EnvironmentPropertiesProperty(
                property_groups=env_props
            ),
            vpc_configurations=[
                flink.CfnApplication.VpcConfigurationProperty(
                    subnet_ids=[s.subnet_id for s in vpc.private_subnets[:2]],
                    security_group_ids=[flink_sg.security_group_id],
                )
            ],
            application_snapshot_configuration=flink.CfnApplication.ApplicationSnapshotConfigurationProperty(
                snapshots_enabled=False  # Disabled for demo — faster restart
            ),
        ),
    )


class ProcessingStack(Stack):

    def __init__(self, scope: Construct, construct_id: str,
                 vpc: ec2.Vpc, flink_sg: ec2.SecurityGroup,
                 msk_cluster_arn: str,
                 flink_artifacts_bucket: s3.Bucket,
                 neptune_endpoint: str,
                 opensearch_endpoint: str,
                 config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        region = self.region
        account = self.account

        # ── Flink Execution Role ──────────────────────────────────────────────
        self.flink_role = iam.Role(
            self, "FlinkRole",
            role_name="fraud-demo-flink-execution",
            assumed_by=iam.ServicePrincipal("kinesisanalytics.amazonaws.com"),
        )

        # MSK
        self.flink_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "kafka-cluster:Connect",
                "kafka-cluster:DescribeCluster",
                "kafka-cluster:DescribeTopic",
                "kafka-cluster:ReadData",
                "kafka-cluster:WriteData",
                "kafka-cluster:CreateTopic",
                "kafka-cluster:DescribeGroup",
                "kafka-cluster:AlterGroup",
            ],
            resources=[msk_cluster_arn, f"{msk_cluster_arn}/*"],
        ))

        # SageMaker inference
        self.flink_role.add_to_policy(iam.PolicyStatement(
            actions=["sagemaker:InvokeEndpoint"],
            resources=[f"arn:aws:sagemaker:{region}:{account}:endpoint/fraud-demo-*"],
        ))

        # DynamoDB
        self.flink_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem",
                "dynamodb:Query", "dynamodb:BatchGetItem", "dynamodb:BatchWriteItem",
            ],
            resources=[f"arn:aws:dynamodb:{region}:{account}:table/fraud-demo-*"],
        ))

        # Neptune
        self.flink_role.add_to_policy(iam.PolicyStatement(
            actions=["neptune-db:*"],
            resources=[f"arn:aws:neptune-db:{region}:{account}:*/*"],
        ))

        # OpenSearch
        self.flink_role.add_to_policy(iam.PolicyStatement(
            actions=["es:ESHttpPost", "es:ESHttpPut", "es:ESHttpGet"],
            resources=[f"arn:aws:es:{region}:{account}:domain/fraud-demo-search/*"],
        ))

        # S3 (artifacts + enrichment)
        flink_artifacts_bucket.grant_read(self.flink_role)

        # VPC + networking
        self.flink_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "ec2:DescribeVpcs", "ec2:DescribeSubnets", "ec2:DescribeSecurityGroups",
                "ec2:DescribeNetworkInterfaces", "ec2:CreateNetworkInterface",
                "ec2:CreateNetworkInterfacePermission", "ec2:DeleteNetworkInterface",
            ],
            resources=["*"],
        ))

        # CloudWatch Logs
        self.flink_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents",
                "logs:DescribeLogGroups", "logs:DescribeLogStreams",
            ],
            resources=["*"],
        ))

        # ── Shared Kafka property groups ──────────────────────────────────────
        def _kafka_source(topic: str, group_id: str):
            return flink.CfnApplication.PropertyGroupProperty(
                property_group_id="KafkaSource",
                property_map={
                    "bootstrap.servers": "${MSK_BOOTSTRAP_SERVERS}",
                    "topic": topic,
                    "group.id": group_id,
                    "security.protocol": "SASL_SSL",
                    "sasl.mechanism": "AWS_MSK_IAM",
                },
            )

        def _kafka_sink(topic: str = "alerts.fraud"):
            return flink.CfnApplication.PropertyGroupProperty(
                property_group_id="KafkaSink",
                property_map={
                    "bootstrap.servers": "${MSK_BOOTSTRAP_SERVERS}",
                    "topic": topic,
                    "security.protocol": "SASL_SSL",
                    "sasl.mechanism": "AWS_MSK_IAM",
                },
            )

        def _sagemaker_props(endpoint_name: str):
            return flink.CfnApplication.PropertyGroupProperty(
                property_group_id="SageMaker",
                property_map={
                    "endpoint.name": endpoint_name,
                    "region": "${AWS_REGION}",
                    "score.threshold": "0.6",
                },
            )

        def _dynamo_props():
            return flink.CfnApplication.PropertyGroupProperty(
                property_group_id="DynamoDB",
                property_map={
                    "region": "${AWS_REGION}",
                    "entity_state_table": "fraud-demo-entity-state",
                    "velocity_table": "fraud-demo-velocity",
                    "alerts_table": "fraud-demo-alerts",
                },
            )

        # ── App 1: Coordinated Trading Detection ─────────────────────────────
        self.coordinated_app = _flink_app(
            self, "CoordinatedTradingApp",
            app_name="fraud-demo-coordinated-trading",
            jar_key="flink-apps/coordinated-trading-1.0.0.jar",
            parallelism=4,
            role_arn=self.flink_role.role_arn,
            bucket_arn=flink_artifacts_bucket.bucket_arn,
            vpc=vpc, flink_sg=flink_sg,
            env_props=[
                _kafka_source("trades.raw", "flink-coordinated-trading"),
                _kafka_sink("alerts.fraud"),
                _sagemaker_props("fraud-demo-coordinated-trading"),
                _dynamo_props(),
                flink.CfnApplication.PropertyGroupProperty(
                    property_group_id="Neptune",
                    property_map={
                        "endpoint": neptune_endpoint,
                        "port": "8182",
                        "cluster.window.seconds": "60",
                        "min.cluster.size": "3",
                        "sync.timing.threshold.ms": "500",
                    },
                ),
                flink.CfnApplication.PropertyGroupProperty(
                    property_group_id="Rules",
                    property_map={
                        "velocity.window.seconds": "60",
                        "velocity.threshold.same_instrument_same_dir": "5",
                        "spread.abuse.min.pips": "2",
                        "correlation.window.seconds": "30",
                    },
                ),
            ],
        )

        # ── App 2: System Abuse & Registration Abuse Detection ────────────────
        self.system_abuse_app = _flink_app(
            self, "SystemAbuseApp",
            app_name="fraud-demo-system-abuse",
            jar_key="flink-apps/system-abuse-1.0.0.jar",
            parallelism=2,
            role_arn=self.flink_role.role_arn,
            bucket_arn=flink_artifacts_bucket.bucket_arn,
            vpc=vpc, flink_sg=flink_sg,
            env_props=[
                flink.CfnApplication.PropertyGroupProperty(
                    property_group_id="KafkaSource",
                    property_map={
                        "bootstrap.servers": "${MSK_BOOTSTRAP_SERVERS}",
                        "topics": "registrations.raw,api.calls",
                        "group.id": "flink-system-abuse",
                        "security.protocol": "SASL_SSL",
                        "sasl.mechanism": "AWS_MSK_IAM",
                    },
                ),
                _kafka_sink("alerts.fraud"),
                _sagemaker_props("fraud-demo-registration-anomaly"),
                _dynamo_props(),
                flink.CfnApplication.PropertyGroupProperty(
                    property_group_id="Rules",
                    property_map={
                        "api.rate.limit.per.min": "200",
                        "order.cancel.rate.threshold": "0.8",
                        "registration.burst.per.ip.24h": "5",
                        "bonus.claim.window.days": "3",
                        "bot.session.entropy.min": "0.3",
                    },
                ),
            ],
        )

        # ── App 3: Account Takeover Detection ────────────────────────────────
        self.ato_app = _flink_app(
            self, "AccountTakeoverApp",
            app_name="fraud-demo-account-takeover",
            jar_key="flink-apps/account-takeover-1.0.0.jar",
            parallelism=2,
            role_arn=self.flink_role.role_arn,
            bucket_arn=flink_artifacts_bucket.bucket_arn,
            vpc=vpc, flink_sg=flink_sg,
            env_props=[
                _kafka_source("sessions.raw", "flink-account-takeover"),
                _kafka_sink("alerts.fraud"),
                _sagemaker_props("fraud-demo-login-risk"),
                _dynamo_props(),
                flink.CfnApplication.PropertyGroupProperty(
                    property_group_id="Rules",
                    property_map={
                        "geo.velocity.impossible.kph": "900",
                        "new.device.withdrawal.window.hours": "24",
                        "pw.reset.to.transfer.window.minutes": "60",
                        "credential.stuffing.failed.threshold": "10",
                        "credential.stuffing.window.minutes": "5",
                    },
                ),
                flink.CfnApplication.PropertyGroupProperty(
                    property_group_id="OpenSearch",
                    property_map={
                        "endpoint": opensearch_endpoint,
                        "index.prefix": "fraud-alerts",
                    },
                ),
            ],
        )

        # ── Outputs ──────────────────────────────────────────────────────────
        CfnOutput(self, "FlinkRoleArn",
                  value=self.flink_role.role_arn,
                  export_name="FraudDemo-FlinkRoleArn")
