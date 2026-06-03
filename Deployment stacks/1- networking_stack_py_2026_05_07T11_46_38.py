
from aws_cdk import (
    Stack, CfnOutput, Tags,
    aws_ec2 as ec2,
)
from constructs import Construct


class NetworkingStack(Stack):
    """VPC and network infrastructure for Fraud Detection Demo."""

    def __init__(self, scope: Construct, construct_id: str, config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # --- VPC ---
        self.vpc = ec2.Vpc(
            self, "FraudDetectionVpc",
            vpc_name="fraud-detection-demo-vpc",
            ip_addresses=ec2.IpAddresses.cidr("[IP_ADDRESS]"),
            max_azs=3,
            nat_gateways=2,  # HA NAT for private subnet egress
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

        # --- VPC Endpoints (cost optimization + security) ---
        # S3 Gateway Endpoint
        self.vpc.add_gateway_endpoint(
            "S3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )

        # DynamoDB Gateway Endpoint
        self.vpc.add_gateway_endpoint(
            "DynamoDBEndpoint",
            service=ec2.GatewayVpcEndpointAwsService.DYNAMODB,
        )

        # SageMaker Runtime Interface Endpoint
        self.vpc.add_interface_endpoint(
            "SageMakerRuntimeEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.SAGEMAKER_RUNTIME,
        )

        # Secrets Manager Interface Endpoint
        self.vpc.add_interface_endpoint(
            "SecretsManagerEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
        )

        # CloudWatch Logs Interface Endpoint
        self.vpc.add_interface_endpoint(
            "CloudWatchLogsEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
        )

        # --- Security Groups ---
        self.msk_sg = ec2.SecurityGroup(
            self, "MskSecurityGroup",
            vpc=self.vpc,
            description="Security group for MSK brokers",
            security_group_name="fraud-demo-msk-sg",
        )

        self.flink_sg = ec2.SecurityGroup(
            self, "FlinkSecurityGroup",
            vpc=self.vpc,
            description="Security group for Flink applications",
            security_group_name="fraud-demo-flink-sg",
        )

        self.rds_sg = ec2.SecurityGroup(
            self, "RdsSecurityGroup",
            vpc=self.vpc,
            description="Security group for RDS instances",
            security_group_name="fraud-demo-rds-sg",
        )

        self.neptune_sg = ec2.SecurityGroup(
            self, "NeptuneSecurityGroup",
            vpc=self.vpc,
            description="Security group for Neptune cluster",
            security_group_name="fraud-demo-neptune-sg",
        )

        self.opensearch_sg = ec2.SecurityGroup(
            self, "OpenSearchSecurityGroup",
            vpc=self.vpc,
            description="Security group for OpenSearch domain",
            security_group_name="fraud-demo-opensearch-sg",
        )

        self.lambda_sg = ec2.SecurityGroup(
            self, "LambdaSecurityGroup",
            vpc=self.vpc,
            description="Security group for Lambda functions",
            security_group_name="fraud-demo-lambda-sg",
        )

        # --- Security Group Rules ---
        # Flink → MSK (Kafka ports)
        self.msk_sg.add_ingress_rule(
            peer=self.flink_sg,
            connection=ec2.Port.tcp_range(9092, 9098),
            description="Flink to MSK (plaintext + TLS + IAM)",
        )

        # Lambda → MSK
        self.msk_sg.add_ingress_rule(
            peer=self.lambda_sg,
            connection=ec2.Port.tcp_range(9092, 9098),
            description="Lambda to MSK",
        )

        # Flink → RDS (PostgreSQL)
        self.rds_sg.add_ingress_rule(
            peer=self.flink_sg,
            connection=ec2.Port.tcp(5432),
            description="Flink to RDS PostgreSQL",
        )

        # Lambda → RDS
        self.rds_sg.add_ingress_rule(
            peer=self.lambda_sg,
            connection=ec2.Port.tcp(5432),
            description="Lambda to RDS PostgreSQL",
        )

        # Flink → Neptune (Gremlin/SPARQL)
        self.neptune_sg.add_ingress_rule(
            peer=self.flink_sg,
            connection=ec2.Port.tcp(8182),
            description="Flink to Neptune",
        )

        # Lambda → Neptune
        self.neptune_sg.add_ingress_rule(
            peer=self.lambda_sg,
            connection=ec2.Port.tcp(8182),
            description="Lambda to Neptune",
        )

        # Flink → OpenSearch
        self.opensearch_sg.add_ingress_rule(
            peer=self.flink_sg,
            connection=ec2.Port.tcp(443),
            description="Flink to OpenSearch",
        )

        # Lambda → OpenSearch
        self.opensearch_sg.add_ingress_rule(
            peer=self.lambda_sg,
            connection=ec2.Port.tcp(443),
            description="Lambda to OpenSearch",
        )

        # --- Outputs ---
        CfnOutput(self, "VpcId", value=self.vpc.vpc_id)
        CfnOutput(self, "PrivateSubnets",
                  value=",".join([s.subnet_id for s in self.vpc.private_subnets]))

        # --- Tags ---
        Tags.of(self).add("Project", "FraudDetectionDemo")
        Tags.of(self).add("Environment", config.get("environment", "demo"))

