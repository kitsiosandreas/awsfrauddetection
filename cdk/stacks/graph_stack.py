"""
Graph Stack
Amazon Neptune for account relationship graph (coordinated ring detection).
"""
from aws_cdk import (
    Stack, RemovalPolicy, Duration, CfnOutput,
    aws_ec2 as ec2,
    aws_iam as iam,
)
import aws_cdk.aws_neptune_alpha as neptune
from constructs import Construct


class GraphStack(Stack):

    def __init__(self, scope: Construct, construct_id: str,
                 vpc: ec2.Vpc, neptune_sg: ec2.SecurityGroup,
                 config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ── Neptune Cluster ──────────────────────────────────────────────────
        self.neptune_cluster = neptune.DatabaseCluster(
            self, "FraudGraphCluster",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
            instance_type=neptune.InstanceType.R5_LARGE,
            security_groups=[neptune_sg],
            db_cluster_name="fraud-demo-graph",
            instances=1,
            removal_policy=RemovalPolicy.DESTROY,
            iam_authentication=True,
            deletion_protection=False,
            backup_retention=Duration.days(1),
        )

        # ── IAM Role for Neptune Bulk Loader ─────────────────────────────────
        self.neptune_loader_role = iam.Role(
            self, "NeptuneLoaderRole",
            role_name="fraud-demo-neptune-loader",
            assumed_by=iam.ServicePrincipal("rds.amazonaws.com"),
        )
        self.neptune_loader_role.add_to_policy(iam.PolicyStatement(
            actions=["s3:GetObject", "s3:ListBucket"],
            resources=["*"],
        ))

        # ── Outputs ──────────────────────────────────────────────────────────
        CfnOutput(self, "NeptuneEndpoint",
                  value=self.neptune_cluster.cluster_endpoint.socket_address,
                  export_name="FraudDemo-NeptuneEndpoint")
        CfnOutput(self, "NeptuneReadEndpoint",
                  value=self.neptune_cluster.cluster_read_endpoint.socket_address,
                  export_name="FraudDemo-NeptuneReadEndpoint")
