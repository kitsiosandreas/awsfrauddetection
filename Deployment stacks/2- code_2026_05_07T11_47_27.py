
from aws_cdk import (
    Stack, Duration, RemovalPolicy, CfnOutput,
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_secretsmanager as secretsmanager,
)
from constructs import Construct


class DataStorageStack(Stack):
    """RDS, S3, and DynamoDB resources for Fraud Detection Demo."""

    def __init__(self, scope: Construct, construct_id: str,
                 vpc: ec2.Vpc, rds_sg: ec2.SecurityGroup, config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ===== AMAZON RDS (PostgreSQL) =====
        # Database credentials in Secrets Manager
        self.db_credentials = rds.DatabaseSecret(
            self, "DbCredentials",
            username="fraud_admin",
            secret_name="fraud-demo/rds/credentials",
        )

        # Parameter group for CDC (logical replication)
        param_group = rds.ParameterGroup(
            self, "PostgresParams",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_15_4
            ),
            parameters={
                "rds.logical_replication": "1",       # Required for Debezium CDC
                "max_replication_slots": "10",
                "max_wal_senders": "10",
                "wal_level": "logical",
            },
        )

        self.rds_instance = rds.DatabaseInstance(
            self, "FraudDemoDb",
            instance_identifier="fraud-detection-demo-db",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_15_4
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.R5, ec2.InstanceSize.LARGE
            ),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            ),
            security_groups=[rds_sg],
            credentials=rds.Credentials.from_secret(self.db_credentials),
            parameter_group=param_group,
            database_name="fraud_detection",
            allocated_storage=100,
            max_allocated_storage=200,
            storage_type=rds.StorageType.GP3,
            multi_az=False,  # Demo environment — single AZ
            deletion_protection=False,
            removal_policy=RemovalPolicy.DESTROY,
            backup_retention=Duration.days(1),
            publicly_accessible=False,
        )

        # ===== AMAZON S3 BUCKETS =====
        # Raw data lake bucket
        self.raw_data_bucket = s3.Bucket(
            self, "RawDataBucket",
            bucket_name=f"fraud-demo-raw-data-{self.account}-{self.region}",
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="ExpireOldData",
                    expiration=Duration.days(30),
                )
            ],
        )

        # ML model artifacts bucket
        self.ml_artifacts_bucket = s3.Bucket(
            self, "MlArtifactsBucket",
            bucket_name=f"fraud-demo-ml-artifacts-{self.account}-{self.region}",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        # Flink application artifacts bucket
        self.flink_artifacts_bucket = s3.Bucket(
            self, "FlinkArtifactsBucket",
            bucket_name=f"fraud-demo-flink-artifacts-{self.account}-{self.region}",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        # Enrichment data bucket (GeoIP, watchlists, device fingerprint DB)
        self.enrichment_bucket = s3.Bucket(
            self, "EnrichmentBucket",
            bucket_name=f"fraud-demo-enrichment-{self.account}-{self.region}",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        # ===== AMAZON DYNAMODB TABLES =====
        # Real-time entity state (user profiles, velocity counters)
        self.entity_state_table = dynamodb.Table(
            self, "EntityStateTable",
            table_name="fraud-demo-entity-state",
            partition_key=dynamodb.Attribute(
                name="entity_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="entity_type", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl",
        )

        # Session context store
        self.session_table = dynamodb.Table(
            self, "SessionTable",
            table_name="fraud-demo-sessions",
            partition_key=dynamodb.Attribute(
                name="session_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl",
        )

        # Velocity counters (sliding window counters for rules)
        self.velocity_table = dynamodb.Table(
            self, "VelocityTable",
            table_name="fraud-demo-velocity",
            partition_key=dynamodb.Attribute(
                name="counter_key", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="window_start", type=dynamodb.AttributeType.NUMBER
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl",
        )

        # Alert history
        self.alerts_table = dynamodb.Table(
            self, "AlertsTable",
            table_name="fraud-demo-alerts",
            partition_key=dynamodb.Attribute(
                name="alert_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="created_at", type=dynamodb.AttributeType.NUMBER
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            global_secondary_indexes=[
                dynamodb.GlobalSecondaryIndex(
                    index_name="by-entity",
                    partition_key=dynamodb.Attribute(
                        name="entity_id", type=dynamodb.AttributeType.STRING
                    ),
                    sort_key=dynamodb.Attribute(
                        name="created_at", type=dynamodb.AttributeType.NUMBER
                    ),
                ),
                dynamodb.GlobalSecondaryIndex(
                    index_name="by-typology",
                    partition_key=dynamodb.Attribute(
                        name="typology", type=dynamodb.AttributeType.STRING
                    ),
                    sort_key=dynamodb.Attribute(
                        name="created_at", type=dynamodb.AttributeType.NUMBER
                    ),
                ),
            ],
        )

        # ===== OUTPUTS =====
        CfnOutput(self, "RdsEndpoint", value=self.rds_instance.db_instance_endpoint_address)
        CfnOutput(self, "RawDataBucketName", value=self.raw_data_bucket.bucket_name)
        CfnOutput(self, "MlArtifactsBucketName", value=self.ml_artifacts_bucket.bucket_name)

