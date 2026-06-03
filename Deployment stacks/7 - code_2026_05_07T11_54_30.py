
from aws_cdk import (
    Stack, CfnOutput,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_kinesisanalyticsv2 as flink,
    aws_s3 as s3,
    aws_logs as logs,
)
from constructs import Construct


class ProcessingStack(Stack):
    """Amazon Managed Service for Apache Flink — stream processing jobs."""

    def __init__(self, scope: Construct, construct_id: str,
                 vpc: ec2.Vpc, flink_sg: ec2.SecurityGroup,
                 msk_cluster_arn: str,
                 flink_artifacts_bucket: s3.Bucket,
                 config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ===== FLINK EXECUTION ROLE =====
        self.flink_role = iam.Role(
            self, "FlinkExecutionRole",
            role_name="fraud-demo-flink-execution",
            assumed_by=iam.ServicePrincipal("kinesisanalytics.amazonaws.com"),
        )

        # MSK Access
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
            resources=[
                msk_cluster_arn,
                f"{msk_cluster_arn}/*",
            ],
        ))

        # SageMaker Endpoint Invocation
        self.flink_role.add_to_policy(iam.PolicyStatement(
            actions=["sagemaker:InvokeEndpoint"],
            resources=[f"arn:aws:sagemaker:{self.region}:{self.account}:endpoint/fraud-demo-*"],
        ))

        # DynamoDB Access
        self.flink_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:UpdateItem",
                "dynamodb:Query",
                "dynamodb:BatchGetItem",
                "dynamodb:BatchWriteItem",
            ],
            resources=[f"arn:aws:dynamodb:{self.region}:{self.account}:table/fraud-demo-*"],
        ))

        # Neptune Access
        self.flink_role.add_to_policy(iam.PolicyStatement(
            actions=["neptune-db:*"],
            resources=[f"arn:aws:neptune-db:{self.region}:{self.account}:*/*"],
        ))

        # S3 Access (Flink artifacts + enrichment data)
        flink_artifacts_bucket.grant_read(self.flink_role)

        # VPC Access
        self.flink_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "ec2:DescribeVpcs",
                "ec2:DescribeSubnets",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeNetworkInterfaces",
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",
            ],
            resources=["*"],
        ))

        # CloudWatch Logs
        self.flink_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams",
            ],
            resources=["*"],
        ))

        # ===== FLINK APPLICATION: COORDINATED TRADING DETECTION =====
        coord_log_group = logs.LogGroup(
            self, "CoordinatedTradingLogGroup",
            log_group_name="/aws/flink/fraud-demo-coordinated-trading",
            retention=logs.RetentionDays.ONE_WEEK,
        )

        self.coordinated_trading_app = flink.CfnApplication(
            self, "CoordinatedTradingApp",
            application_name="fraud-demo-coordinated-trading",
            runtime_environment="FLINK-1_18",
            service_execution_role=self.flink_role.role_arn,
            application_configuration=flink.CfnApplication.ApplicationConfigurationProperty(
                application_code_configuration=flink.CfnApplication.ApplicationCodeConfigurationProperty(
                    code_content=flink.CfnApplication.CodeContentProperty(
                        s3_content_location=flink.CfnApplication.S3ContentLocationProperty(
                            bucket_arn=flink_artifacts_bucket.bucket_arn,
                            file_key="flink-apps/coordinated-trading-1.0.0.jar",
                        ),
                    ),
                    code_content_type="ZIPFILE",
                ),
                flink_application_configuration=flink.CfnApplication.FlinkApplicationConfigurationProperty(
                    parallelism_configuration=flink.CfnApplication.ParallelismConfigurationProperty(
                        configuration_type="CUSTOM",
                        parallelism=4,
                        parallelism_per_kpu=1,
                        auto_scaling_enabled=False,
                    ),
                    checkpoint_configuration=flink.CfnApplication.CheckpointConfigurationProperty(
                        configuration_type="CUSTOM",
                        checkpointing_enabled=True,
                        checkpoint_interval=60000,  # 60 seconds
                        min_pause_between_checkpoints=30000,
                    ),
                    monitoring_configuration=flink.CfnApplication.MonitoringConfigurationProperty(
                        configuration_type="CUSTOM",
                        log_level="INFO",
                        metrics_level="APPLICATION",
                    ),
                ),
                environment_properties=flink.CfnApplication.EnvironmentPropertiesProperty(
                    property_groups=[
                        flink.CfnApplication.PropertyGroupProperty(
                            property_group_id="KafkaSource",
                            property_map={
                                "bootstrap.servers": "${MSK_BOOTSTRAP_SERVERS}",
                                "topic": "trades.raw",
                                "group.id": "flink-coordinated-trading",
                            },
                        ),
                        flink.CfnApplication.PropertyGroupProperty(
                            property_group_id="KafkaSink",
                            property_map={
                                "bootstrap.servers": "${MSK_BOOTSTRAP_SERVERS}",
                                "topic": "alerts.fraud",
                            },
                        ),
                        flink.CfnApplication.PropertyGroupProperty(
                            property_group_id="SageMaker",
                            property_map={
                                "endpoint.name": "fraud-demo-coordinated-trading",
                                "region": "${AWS_REGION}",
                            },
                        ),
                        flink.CfnApplication.PropertyGroupProperty(
                            property_group_id="Neptune",
                            property_map={
                                "endpoint": "${NEPTUNE_ENDPOINT}",
                                "port": "8182",
                            },
                        ),
                    ],
                ),
                vpc_configurations=[
                    flink.CfnApplication.VpcConfigurationProperty(
                        subnet_ids=[s.subnet_id for s in vpc.private_subnets[:2]],
                        security_group_ids=[flink_sg.security_group_id],
                    ),
                ],
            ),
        )

        # ===== FLINK APPLICATION: SYSTEM ABUSE DETECTION =====
        self.system_abuse_app = flink.CfnApplication(
            self, "SystemAbuseApp",
            application_name="fraud-demo-system-abuse",
            runtime_environment="FLINK-1_18",
            service_execution_role=self.flink_role.role_arn,
            application_configuration=flink.CfnApplication.ApplicationConfigurationProperty(
                application_code_configuration=flink.CfnApplication.ApplicationCodeConfigurationProperty(
                    code_content=flink.CfnApplication.CodeContentProperty(
                        s3_content_location=flink.CfnApplication.S3ContentLocationProperty(
                            bucket_arn=flink_artifacts_bucket.bucket_arn,
                            file_key="flink-apps/system-abuse-1.0.0.jar",
                        ),
                    ),
                    code_content_type="ZIPFILE",
                ),
                flink_application_configuration=flink.CfnApplication.FlinkApplicationConfigurationProperty(
                    parallelism_configuration=flink.CfnApplication.ParallelismConfigurationProperty(
                        configuration_type="CUSTOM",
                        parallelism=2,
                        parallelism_per_kpu=1,
                        auto_scaling_enabled=False,
                    ),
                    checkpoint_configuration=flink.CfnApplication.CheckpointConfigurationProperty(
                        configuration_type="CUSTOM",
                        checkpointing_enabled=True,
                        checkpoint_interval=60000,
                        min_pause_between_checkpoints=30000,
                    ),
                ),
                environment_properties=flink.CfnApplication.EnvironmentPropertiesProperty(
                    property_groups=[
                        flink.CfnApplication.PropertyGroupProperty(
                            property_group_id="KafkaSource",
                            property_map={
                                "bootstrap.servers": "${MSK_BOOTSTRAP_SERVERS}",
                                "topics": "registrations.raw,api.calls",
                                "group.id": "flink-system-abuse",
                            },
                        ),
                        flink.CfnApplication.PropertyGroupProperty(
                            property_group_id="KafkaSink",
                            property_map={
                                "bootstrap.servers": "${MSK_BOOTSTRAP_SERVERS}",
                                "topic": "alerts.fraud",
                            },
                        ),
                        flink.CfnApplication.PropertyGroupProperty(
                            property_group_id="SageMaker",
                            property_map={
                                "endpoint.name": "fraud-demo-registration-anomaly",
                                "region": "${AWS_REGION}",
                            },
                        ),
                    ],
                ),
                vpc_configurations=[
                    flink.CfnApplication.VpcConfigurationProperty(
                        subnet_ids=[s.subnet_id for s in vpc.private_subnets[:2]],
                        security_group_ids=[flink_sg.security_group_id],
                    ),
                ],
            ),
        )

        # ===== FLINK APPLICATION: ACCOUNT TAKEOVER DETECTION =====
        self.ato_app = flink.CfnApplication(
            self, "AccountTakeoverApp",
            application_name="fraud-demo-account-takeover",
            runtime_environment="FLINK-1_18",
            service_execution_role=self.flink_role.role_arn,
            application_configuration=flink.CfnApplication.ApplicationConfigurationProperty(
                application_code_configuration=flink.CfnApplication.ApplicationCodeConfigurationProperty(
                    code_content=flink.CfnApplication.CodeContentProperty(
                        s3_content_location=flink.CfnApplication.S3ContentLocationProperty(
                            bucket_arn=flink_artifacts_bucket.bucket_arn,
                            file_key="flink-apps/account-takeover-1.0.0.jar",
                        ),
                    ),
                    code_content_type="ZIPFILE",
                ),
                flink_application_configuration=flink.CfnApplication.FlinkApplicationConfigurationProperty(
                    parallelism_configuration=flink.CfnApplication.ParallelismConfigurationProperty(
                        configuration_type="CUSTOM",
                        parallelism=2,
                        parallelism_per_kpu=1,
                        auto_scaling_enabled=False,
                    ),
                    checkpoint_configuration=flink.CfnApplication.CheckpointConfigurationProperty(
                        configuration_type="CUSTOM",
                        checkpointing_enabled=True,
                        checkpoint_interval=60000,
                        min_pause_between_checkpoints=30000,
                    ),
                ),
                environment_properties=flink.CfnApplication.EnvironmentPropertiesProperty(
                    property_groups=[
                        flink.CfnApplication.PropertyGroupProperty(
                            property_group_id="KafkaSource",
                            property_map={
                                "bootstrap.servers": "${MSK_BOOTSTRAP_SERVERS}",
                                "topic": "sessions.raw",
                                "group.id": "flink-account-takeover",
                            },
                        ),
                        flink.CfnApplication.PropertyGroupProperty(
                            property_group_id="KafkaSink",
                            property_map={
                                "bootstrap.servers": "${MSK_BOOTSTRAP_SERVERS}",
                                "topic": "alerts.fraud",
                            },
                        ),
                        flink.CfnApplication.PropertyGroupProperty(
                            property_group_id="SageMaker",
                            property_map={
                                "endpoint.name": "fraud-demo-login-risk",
                                "region": "${AWS_REGION}",
                            },
                        ),
                    ],
                ),
                vpc_configurations=[
                    flink.CfnApplication.VpcConfigurationProperty(
                        subnet_ids=[s.subnet_id for s in vpc.private_subnets[:2]],
                        security_group_ids=[flink_sg.security_group_id],
                    ),
                ],
            ),
        )

        # ===== OUTPUTS =====
        CfnOutput(self, "FlinkRoleArn", value=self.flink_role.role_arn)

