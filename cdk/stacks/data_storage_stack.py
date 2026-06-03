"""
Data Storage Stack
RDS PostgreSQL (with CDC), S3 buckets, and DynamoDB tables.
"""
from aws_cdk import (
    Stack, Duration, RemovalPolicy, CfnOutput,
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
)
from constructs import Construct


class DataStorageStack(Stack):

    def __init__(self, scope: Construct, construct_id: str,
                 vpc: ec2.Vpc, rds_sg: ec2.SecurityGroup,
                 config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ── RDS ──────────────────────────────────────────────────────────────
        self.db_credentials = rds.DatabaseSecret(
            self, "DbCredentials",
            username="fraud_admin",
            secret_name="fraud-demo/rds/credentials",
        )

        # Logical replication required for Debezium CDC connector
        param_group = rds.ParameterGroup(
            self, "PostgresParams",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_15_4
            ),
            parameters={
                "rds.logical_replication": "1",
                "max_replication_slots": "10",
                "max_wal_senders": "10",
                "wal_level": "logical",
            },
        )

        self.rds_instance = rds.DatabaseInstance(
            self, "FraudDemoDb",
            instance_identifier="fraud-demo-db",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_15_4
            ),
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.R5, ec2.InstanceSize.LARGE),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
            security_groups=[rds_sg],
            credentials=rds.Credentials.from_secret(self.db_credentials),
            parameter_group=param_group,
            database_name="fraud_detection",
            allocated_storage=100,
            max_allocated_storage=200,
            storage_type=rds.StorageType.GP3,
            multi_az=False,
            deletion_protection=False,
            removal_policy=RemovalPolicy.DESTROY,
            backup_retention=Duration.days(1),
            publicly_accessible=False,
            enable_performance_insights=True,
            performance_insight_retention=rds.PerformanceInsightRetention.DEFAULT,
        )

        # ── S3 Buckets ───────────────────────────────────────────────────────
        self.raw_data_bucket = s3.Bucket(
            self, "RawDataBucket",
            bucket_name=f"fraud-demo-raw-{self.account}-{self.region}",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[s3.LifecycleRule(expiration=Duration.days(30))],
        )

        self.ml_artifacts_bucket = s3.Bucket(
            self, "MlArtifactsBucket",
            bucket_name=f"fraud-demo-ml-{self.account}-{self.region}",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        self.flink_artifacts_bucket = s3.Bucket(
            self, "FlinkArtifactsBucket",
            bucket_name=f"fraud-demo-flink-{self.account}-{self.region}",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        self.enrichment_bucket = s3.Bucket(
            self, "EnrichmentBucket",
            bucket_name=f"fraud-demo-enrichment-{self.account}-{self.region}",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        # ── DynamoDB Tables ──────────────────────────────────────────────────
        # Entity state — account profiles & risk scores
        self.entity_state_table = dynamodb.Table(
            self, "EntityStateTable",
            table_name="fraud-demo-entity-state",
            partition_key=dynamodb.Attribute(name="entity_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="entity_type", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl",
        )

        # Session context — active login sessions
        self.session_table = dynamodb.Table(
            self, "SessionTable",
            table_name="fraud-demo-sessions",
            partition_key=dynamodb.Attribute(name="session_id", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl",
        )

        # Velocity counters — sliding window counts per account/IP
        self.velocity_table = dynamodb.Table(
            self, "VelocityTable",
            table_name="fraud-demo-velocity",
            partition_key=dynamodb.Attribute(name="counter_key", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="window_start", type=dynamodb.AttributeType.NUMBER),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl",
        )

        # Alerts — fraud alert history with GSIs for querying
        self.alerts_table = dynamodb.Table(
            self, "AlertsTable",
            table_name="fraud-demo-alerts",
            partition_key=dynamodb.Attribute(name="alert_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="created_at", type=dynamodb.AttributeType.NUMBER),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.alerts_table.add_global_secondary_index(
            index_name="by-entity",
            partition_key=dynamodb.Attribute(name="entity_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="created_at", type=dynamodb.AttributeType.NUMBER),
        )
        self.alerts_table.add_global_secondary_index(
            index_name="by-typology",
            partition_key=dynamodb.Attribute(name="typology", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="created_at", type=dynamodb.AttributeType.NUMBER),
        )
        self.alerts_table.add_global_secondary_index(
            index_name="by-severity",
            partition_key=dynamodb.Attribute(name="severity", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="created_at", type=dynamodb.AttributeType.NUMBER),
        )

        # ── Outputs ──────────────────────────────────────────────────────────
        CfnOutput(self, "RdsEndpoint",
                  value=self.rds_instance.db_instance_endpoint_address,
                  export_name="FraudDemo-RdsEndpoint")
        CfnOutput(self, "RdsSecretArn",
                  value=self.db_credentials.secret_arn,
                  export_name="FraudDemo-RdsSecretArn")
        CfnOutput(self, "RawDataBucket",
                  value=self.raw_data_bucket.bucket_name,
                  export_name="FraudDemo-RawDataBucket")
        CfnOutput(self, "MlArtifactsBucket",
                  value=self.ml_artifacts_bucket.bucket_name,
                  export_name="FraudDemo-MlArtifactsBucket")
