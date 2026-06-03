"""
Search Stack
Amazon OpenSearch Service for fraud investigation UI and dashboards.
"""
from aws_cdk import (
    Stack, RemovalPolicy, SecretValue, CfnOutput,
    aws_ec2 as ec2,
    aws_opensearchservice as opensearch,
    aws_iam as iam,
    aws_secretsmanager as secretsmanager,
)
from constructs import Construct


class SearchStack(Stack):

    def __init__(self, scope: Construct, construct_id: str,
                 vpc: ec2.Vpc, opensearch_sg: ec2.SecurityGroup,
                 config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # Admin password in Secrets Manager
        self.opensearch_secret = secretsmanager.Secret(
            self, "OpenSearchAdminSecret",
            secret_name="fraud-demo/opensearch/admin",
            description="OpenSearch master user credentials",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username": "fraud_admin"}',
                generate_string_key="password",
                exclude_punctuation=True,
                password_length=16,
            ),
        )

        # ── OpenSearch Domain ─────────────────────────────────────────────────
        self.opensearch_domain = opensearch.Domain(
            self, "FraudSearchDomain",
            domain_name="fraud-demo-search",
            version=opensearch.EngineVersion.OPENSEARCH_2_11,
            vpc=vpc,
            vpc_subnets=[ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                availability_zones=[vpc.availability_zones[0]],
            )],
            security_groups=[opensearch_sg],
            capacity=opensearch.CapacityConfig(
                data_nodes=2,
                data_node_instance_type="m5.large.search",
            ),
            ebs=opensearch.EbsOptions(
                volume_size=100,
                volume_type=ec2.EbsDeviceVolumeType.GP3,
            ),
            node_to_node_encryption=True,
            encryption_at_rest=opensearch.EncryptionAtRestOptions(enabled=True),
            enforce_https=True,
            removal_policy=RemovalPolicy.DESTROY,
            fine_grained_access_control=opensearch.AdvancedSecurityOptions(
                master_user_name="fraud_admin",
                master_user_password=self.opensearch_secret.secret_value_from_json("password"),
            ),
            zone_awareness=opensearch.ZoneAwarenessConfig(enabled=False),
            logging=opensearch.LoggingOptions(
                slow_search_log_enabled=True,
                app_log_enabled=True,
            ),
            access_policies=[
                iam.PolicyStatement(
                    principals=[iam.AnyPrincipal()],
                    actions=["es:*"],
                    resources=["*"],
                )
            ],
        )

        # ── Outputs ──────────────────────────────────────────────────────────
        CfnOutput(self, "OpenSearchEndpoint",
                  value=self.opensearch_domain.domain_endpoint,
                  export_name="FraudDemo-OpenSearchEndpoint")
        CfnOutput(self, "OpenSearchDashboardUrl",
                  value=f"https://{self.opensearch_domain.domain_endpoint}/_dashboards",
                  export_name="FraudDemo-OpenSearchDashboardUrl")
        CfnOutput(self, "OpenSearchDomainArn",
                  value=self.opensearch_domain.domain_arn,
                  export_name="FraudDemo-OpenSearchDomainArn")
