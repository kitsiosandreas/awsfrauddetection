
from aws_cdk import (
    Stack, RemovalPolicy, CfnOutput,
    aws_ec2 as ec2,
    aws_neptune_alpha as neptune,
    aws_iam as iam,
)
from constructs import Construct


class GraphStack(Stack):
    """Amazon Neptune for account relationship graph analysis."""

    def __init__(self, scope: Construct, construct_id: str,
                 vpc: ec2.Vpc, neptune_sg: ec2.SecurityGroup,
                 config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ===== NEPTUNE CLUSTER =====
        self.neptune_cluster = neptune.DatabaseCluster(
            self, "FraudGraphCluster",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            ),
            instance_type=neptune.InstanceType.R5_LARGE,
            security_groups=[neptune_sg],
            db_cluster_name="fraud-demo-graph",
            instances=1,  # Single instance for demo
            removal_policy=RemovalPolicy.DESTROY,
            iam_authentication=True,
            # Enable Neptune Analytics for graph algorithms
            deletion_protection=False,
            backup_retention=neptune.Duration.days(1),
        )

        # IAM Role for Neptune bulk loader (initial graph population)
        self.neptune_loader_role = iam.Role(
            self, "NeptuneLoaderRole",
            role_name="fraud-demo-neptune-loader",
            assumed_by=iam.ServicePrincipal("rds.amazonaws.com"),
        )

        # ===== GRAPH SCHEMA DESIGN =====
        # Vertices: Account, Device, IP, Email, Phone, BankAccount, Instrument
        # Edges: USES_DEVICE, LOGS_IN_FROM, REGISTERED_WITH, FUNDS_FROM,
        #         TRADES, CORRELATED_WITH, SAME_CLUSTER
        # Properties: timestamps, risk_scores, cluster_id

        # ===== OUTPUTS =====
        CfnOutput(self, "NeptuneEndpoint",
                  value=self.neptune_cluster.cluster_endpoint.socket_address)
        CfnOutput(self, "NeptuneReadEndpoint",
                  value=self.neptune_cluster.cluster_read_endpoint.socket_address)

