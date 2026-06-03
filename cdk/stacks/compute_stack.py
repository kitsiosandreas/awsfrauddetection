"""
Compute Stack
ECS Fargate data generator + Lambda topic initializer.
"""
from aws_cdk import (
    Stack, Duration, RemovalPolicy, CfnOutput,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_ecr_assets as ecr_assets,
    aws_iam as iam,
    aws_logs as logs,
    aws_s3 as s3,
)
from constructs import Construct
import os


class ComputeStack(Stack):

    def __init__(self, scope: Construct, construct_id: str,
                 vpc: ec2.Vpc,
                 ecs_sg: ec2.SecurityGroup,
                 lambda_sg: ec2.SecurityGroup,
                 msk_cluster_arn: str,
                 rds_secret_arn: str,
                 rds_endpoint: str,
                 raw_data_bucket: s3.Bucket,
                 enrichment_bucket: s3.Bucket,
                 config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        scenario   = config.get("generator_scenario", "mixed")
        tps        = str(config.get("generator_tps", "200"))
        accounts   = str(config.get("generator_accounts", "1000"))
        fraud_rate = str(config.get("fraud_injection_rate", "0.15"))

        # ── ECS Cluster ──────────────────────────────────────────────────────
        self.cluster = ecs.Cluster(
            self, "DataGenCluster",
            cluster_name="fraud-demo-datagen",
            vpc=vpc,
            container_insights=True,
        )

        # ── IAM Task Role ─────────────────────────────────────────────────────
        task_role = iam.Role(
            self, "DataGenTaskRole",
            role_name="fraud-demo-datagen-task",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )
        task_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "kafka-cluster:Connect",
                "kafka-cluster:WriteData",
                "kafka-cluster:CreateTopic",
                "kafka-cluster:DescribeTopic",
                "kafka-cluster:DescribeCluster",
            ],
            resources=[msk_cluster_arn, f"{msk_cluster_arn}/*"],
        ))
        task_role.add_to_policy(iam.PolicyStatement(
            actions=["secretsmanager:GetSecretValue"],
            resources=[rds_secret_arn],
        ))
        raw_data_bucket.grant_write(task_role)
        enrichment_bucket.grant_read(task_role)

        # ── Task Execution Role (ECR pull + CW logs) ──────────────────────────
        exec_role = iam.Role(
            self, "DataGenExecRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                ),
            ],
        )

        # ── Docker Image ──────────────────────────────────────────────────────
        # The Dockerfile lives in ../data_generator relative to the CDK app
        image_asset = ecr_assets.DockerImageAsset(
            self, "DataGenImage",
            directory=os.path.join(os.path.dirname(__file__), "..", "..", "data_generator"),
        )

        # ── Fargate Task Definition ───────────────────────────────────────────
        task_def = ecs.FargateTaskDefinition(
            self, "DataGenTaskDef",
            family="fraud-demo-datagen",
            cpu=1024,
            memory_limit_mib=2048,
            task_role=task_role,
            execution_role=exec_role,
        )

        log_group = logs.LogGroup(
            self, "DataGenLogGroup",
            log_group_name="/ecs/fraud-demo-datagen",
            removal_policy=RemovalPolicy.DESTROY,
            retention=logs.RetentionDays.ONE_WEEK,
        )

        task_def.add_container(
            "DataGenerator",
            image=ecs.ContainerImage.from_docker_image_asset(image_asset),
            logging=ecs.LogDriver.aws_logs(
                stream_prefix="datagen",
                log_group=log_group,
            ),
            environment={
                "SCENARIO":           scenario,
                "TPS":                tps,
                "TOTAL_ACCOUNTS":     accounts,
                "FRAUD_RATE":         fraud_rate,
                "RDS_ENDPOINT":       rds_endpoint,
                "RDS_DB":             "fraud_detection",
                "RAW_DATA_BUCKET":    raw_data_bucket.bucket_name,
                "ENRICHMENT_BUCKET":  enrichment_bucket.bucket_name,
                "AWS_DEFAULT_REGION": self.region,
                "MSK_CLUSTER_ARN":    msk_cluster_arn,
            },
            secrets={
                "RDS_SECRET_ARN": ecs.Secret.from_secrets_manager(
                    # resolved at runtime — not echoed into the task def
                    # use the ARN string; Lambda/ECS resolves it via SecretsManager
                    # We pass the ARN as env var and let the app fetch it
                ),
            } if False else {},  # Secrets injected via env var ARN + app-side fetch
            essential=True,
        )

        # ── Fargate Service (runs continuously) ───────────────────────────────
        self.data_gen_service = ecs.FargateService(
            self, "DataGenService",
            cluster=self.cluster,
            task_definition=task_def,
            service_name="fraud-demo-datagen",
            desired_count=1,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            security_groups=[ecs_sg],
            assign_public_ip=False,
        )

        # ── Outputs ──────────────────────────────────────────────────────────
        CfnOutput(self, "EcsClusterName",
                  value=self.cluster.cluster_name,
                  export_name="FraudDemo-EcsClusterName")
        CfnOutput(self, "DataGenServiceName",
                  value=self.data_gen_service.service_name,
                  export_name="FraudDemo-DataGenServiceName")
        CfnOutput(self, "DataGenLogGroup",
                  value=log_group.log_group_name,
                  export_name="FraudDemo-DataGenLogGroup")
