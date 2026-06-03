"""
Networking Stack
VPC, subnets, NAT gateways, VPC endpoints, and security groups.
"""
from aws_cdk import Stack, CfnOutput, Tags, aws_ec2 as ec2
from constructs import Construct


class NetworkingStack(Stack):
    """VPC and network infrastructure for Fraud Detection Demo."""

    def __init__(self, scope: Construct, construct_id: str, config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ── VPC ─────────────────────────────────────────────────────────────
        self.vpc = ec2.Vpc(
            self, "FraudDetectionVpc",
            vpc_name="fraud-demo-vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"),
            max_azs=3,
            nat_gateways=2,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=22,
                ),
                ec2.SubnetConfiguration(
                    name="Isolated",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                ),
            ],
        )

        # ── VPC Endpoints (reduce NAT costs + keep traffic private) ─────────
        self.vpc.add_gateway_endpoint(
            "S3Endpoint", service=ec2.GatewayVpcEndpointAwsService.S3
        )
        self.vpc.add_gateway_endpoint(
            "DynamoDBEndpoint", service=ec2.GatewayVpcEndpointAwsService.DYNAMODB
        )
        self.vpc.add_interface_endpoint(
            "SageMakerRuntimeEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.SAGEMAKER_RUNTIME,
        )
        self.vpc.add_interface_endpoint(
            "SecretsManagerEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
        )
        self.vpc.add_interface_endpoint(
            "CloudWatchLogsEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
        )

        # ── Security Groups ──────────────────────────────────────────────────
        self.msk_sg = ec2.SecurityGroup(
            self, "MskSg", vpc=self.vpc,
            description="MSK broker security group",
            security_group_name="fraud-demo-msk-sg",
        )
        self.flink_sg = ec2.SecurityGroup(
            self, "FlinkSg", vpc=self.vpc,
            description="Flink application security group",
            security_group_name="fraud-demo-flink-sg",
        )
        self.rds_sg = ec2.SecurityGroup(
            self, "RdsSg", vpc=self.vpc,
            description="RDS PostgreSQL security group",
            security_group_name="fraud-demo-rds-sg",
        )
        self.neptune_sg = ec2.SecurityGroup(
            self, "NeptuneSg", vpc=self.vpc,
            description="Neptune cluster security group",
            security_group_name="fraud-demo-neptune-sg",
        )
        self.opensearch_sg = ec2.SecurityGroup(
            self, "OpenSearchSg", vpc=self.vpc,
            description="OpenSearch domain security group",
            security_group_name="fraud-demo-opensearch-sg",
        )
        self.lambda_sg = ec2.SecurityGroup(
            self, "LambdaSg", vpc=self.vpc,
            description="Lambda function security group",
            security_group_name="fraud-demo-lambda-sg",
        )
        self.ecs_sg = ec2.SecurityGroup(
            self, "EcsSg", vpc=self.vpc,
            description="ECS data generator security group",
            security_group_name="fraud-demo-ecs-sg",
        )

        # ── Security Group Rules ─────────────────────────────────────────────
        for source_sg in [self.flink_sg, self.lambda_sg, self.ecs_sg]:
            self.msk_sg.add_ingress_rule(
                source_sg, ec2.Port.tcp_range(9092, 9098),
                "Allow Kafka (plaintext + TLS + IAM)",
            )

        for source_sg in [self.flink_sg, self.lambda_sg, self.ecs_sg]:
            self.rds_sg.add_ingress_rule(
                source_sg, ec2.Port.tcp(5432), "Allow PostgreSQL",
            )

        for source_sg in [self.flink_sg, self.lambda_sg]:
            self.neptune_sg.add_ingress_rule(
                source_sg, ec2.Port.tcp(8182), "Allow Neptune Gremlin",
            )
            self.opensearch_sg.add_ingress_rule(
                source_sg, ec2.Port.tcp(443), "Allow OpenSearch HTTPS",
            )

        # ── Outputs ──────────────────────────────────────────────────────────
        CfnOutput(self, "VpcId", value=self.vpc.vpc_id, export_name="FraudDemo-VpcId")
        CfnOutput(
            self, "PrivateSubnetIds",
            value=",".join(s.subnet_id for s in self.vpc.private_subnets),
            export_name="FraudDemo-PrivateSubnetIds",
        )

        Tags.of(self).add("Project", "FraudDetectionDemo")
        Tags.of(self).add("Environment", config.get("environment", "demo"))
