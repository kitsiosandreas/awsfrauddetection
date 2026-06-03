
from aws_cdk import (
    Stack, Duration, RemovalPolicy, CfnOutput,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_ecs_patterns as ecs_patterns,
    aws_ecr_assets as ecr_assets,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_logs as logs,
    aws_s3 as s3,
)
from constructs import Construct
import os


class ComputeStack(Stack):
    """Lambda functions and ECS-based synthetic data generator."""

    def __init__(self, scope: Construct, construct_id: str,
                 vpc: ec2.Vpc, lambda_sg: ec2.SecurityGroup,
                 msk_cluster_arn: str,
                 config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ===== ECS CLUSTER (Data Generator) =====
        self.ecs_cluster = ecs.Cluster(
            self, "DataGenCluster",
            cluster_name="fraud-demo-datagen",
            vpc=vpc,
            container_insights=True,
        )

        # Build Docker image for data generator
        data_gen_image = ecr_assets.DockerImageAsset(
            self, "DataGenImage",
            directory=os.path.join(os.path.dirname(__file__), "..", "data_generator"),
        )

        # ECS Fargate Task Definition
        data_gen_task_def = ecs.FargateTaskDefinition(
            self, "DataGenTaskDef",
            family="fraud-demo-datagen",
            cpu=1024,       # 1 vCPU
            memory_limit_mib=2048,  # 2 GB
        )

        # Add MSK IAM permissions to task role
        data_gen_task_def.task_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "kafka-cluster:Connect",
                "kafka-cluster:WriteData",
                "kafka
