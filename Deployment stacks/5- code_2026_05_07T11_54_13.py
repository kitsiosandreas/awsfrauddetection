
from aws_cdk import (
    Stack, RemovalPolicy, CfnOutput,
    aws_ec2 as ec2,
    aws_opensearchservice as opensearch,
    aws_iam as iam,
)
from constructs import Construct


class SearchStack(Stack):
    """Amazon OpenSearch Service for fraud investigation and alerting UI."""

    def __init__(self, scope: Construct, construct_id: str,
                 vpc: ec2.Vpc, opensearch_sg: ec2.SecurityGroup,
                 config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ===== OPENSEARCH DOMAIN =====
        self.opensearch_domain = opensearch.Domain(
            self, "FraudSearchDomain",
            domain_name="fraud-demo-search",
            version=opensearch.EngineVersion.OPENSEARCH_2_11,
            vpc=vpc,
            vpc_subnets=[ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                availability_zones=[vpc.availability_zones[0]],  # Single AZ for demo
            )],
            security_groups=[opensearch_sg],
            capacity=opensearch.CapacityConfig(
                data_nodes=2,
                data_node_instance_type="m5.large.search",
                master_nodes=0,  # No dedicated masters for demo
            ),
            ebs=opensearch.EbsOptions(
                volume_size=100,
                volume_type=ec2.EbsDeviceVolumeType.GP3,
            ),
            node_to_node_encryption=True,
            encryption_at_rest=opensearch.EncryptionAtRestOptions(enabled=True),
            enforce_https=True,
            removal_policy=RemovalPolicy.DESTROY,
            # Fine-grained access control
            fine_grained_access_control=opensearch.AdvancedSecurityOptions(
                master_user_name="fraud_admin",
                master_user_password=None,  # Will use Secrets Manager
            ),
            zone_awareness=opensearch.ZoneAwarenessConfig(enabled=False),
            logging=opensearch.LoggingOptions(
                slow_search_log_enabled=True,
                app_log_enabled=True,
            ),
        )

        # ===== INDEX TEMPLATES =====
        # Managed via custom resource or deployment script
        # Indices:
        #   - fraud-events-{YYYY.MM.DD}  (daily rollover)
        #   - fraud-alerts-{YYYY.MM.DD}
        #   - fraud-sessions-{YYYY.MM.DD}
        #   - fraud-entities (account profiles, always current)

        # ===== OUTPUTS =====
        CfnOutput(self, "OpenSearchEndpoint",
                  value=self.opensearch_domain.domain_endpoint)
        CfnOutput(self, "OpenSearchDashboardUrl",
                  value=f"https://{self.opensearch_domain.domain_endpoint}/_dashboards")

