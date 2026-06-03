
from aws_cdk import (
    Stack, CfnOutput,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_sagemaker as sagemaker,
    aws_s3 as s3,
)
from constructs import Construct


class MLStack(Stack):
    """SageMaker endpoints and Feature Store for ML-based fraud detection."""

    def __init__(self, scope: Construct, construct_id: str,
                 vpc: ec2.Vpc, ml_artifacts_bucket: s3.Bucket,
                 config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ===== SAGEMAKER EXECUTION ROLE =====
        self.sagemaker_role = iam.Role(
            self, "SageMakerExecutionRole",
            role_name="fraud-demo-sagemaker-execution",
            assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSageMakerFullAccess"
                ),
            ],
        )

        ml_artifacts_bucket.grant_read_write(self.sagemaker_role)

        # ===== SAGEMAKER FEATURE STORE =====
        # Feature Group: Account Trading Profile
        self.account_trading_fg = sagemaker.CfnFeatureGroup(
            self, "AccountTradingFeatureGroup",
            feature_group_name="fraud-demo-account-trading-profile",
            record_identifier_feature_name="account_id",
            event_time_feature_name="event_time",
            online_store_config=sagemaker.CfnFeatureGroup.OnlineStoreConfigProperty(
                enable_online_store=True,
            ),
            offline_store_config=sagemaker.CfnFeatureGroup.OfflineStoreConfigProperty(
                s3_storage_config=sagemaker.CfnFeatureGroup.S3StorageConfigProperty(
                    s3_uri=f"s3://{ml_artifacts_bucket.bucket_name}/feature-store/account-trading/",
                ),
            ),
            role_arn=self.sagemaker_role.role_arn,
            feature_definitions=[
                {"feature_name": "account_id", "feature_type": "String"},
                {"feature_name": "event_time", "feature_type": "Fractional"},
                {"feature_name": "avg_trade_size_7d", "feature_type": "Fractional"},
                {"feature_name": "trade_count_24h", "feature_type": "Integral"},
                {"feature_name": "unique_instruments_7d", "feature_type": "Integral"},
                {"feature_name": "avg_hold_duration_min", "feature_type": "Fractional"},
                {"feature_name": "pnl_volatility_7d", "feature_type": "Fractional"},
                {"feature_name": "peak_trading_hour", "feature_type": "Integral"},
                {"feature_name": "instrument_concentration", "feature_type": "Fractional"},
                {"feature_name": "correlated_accounts_count", "feature_type": "Integral"},
                {"feature_name": "cluster_id", "feature_type": "String"},
            ],
        )

        # Feature Group: Login Behavior Profile
        self.login_behavior_fg = sagemaker.CfnFeatureGroup(
            self, "LoginBehaviorFeatureGroup",
            feature_group_name="fraud-demo-login-behavior",
            record_identifier_feature_name="account_id",
            event_time_feature_name="event_time",
            online_store_config=sagemaker.CfnFeatureGroup.OnlineStoreConfigProperty(
                enable_online_store=True,
            ),
            role_arn=self.sagemaker_role.role_arn,
            feature_definitions=[
                {"feature_name": "account_id", "feature_type": "String"},
                {"feature_name": "event_time", "feature_type": "Fractional"},
                {"feature_name": "known_device_count", "feature_type": "Integral"},
                {"feature_name": "known_ip_count", "feature_type": "Integral"},
                {"feature_name": "avg_session_duration_min", "feature_type": "Fractional"},
                {"feature_name": "login_hour_mode", "feature_type": "Integral"},
                {"feature_name": "country_code_primary", "feature_type": "String"},
                {"feature_name": "failed_login_count_7d", "feature_type": "Integral"},
                {"feature_name": "password_reset_count_30d", "feature_type": "Integral"},
            ],
        )

        # ===== SAGEMAKER ENDPOINTS =====
        # NOTE: Models are pre-trained and artifacts stored in S3
        # Endpoints are created via SageMaker SDK in deployment scripts
        # CDK defines the endpoint configurations

        # Endpoint Config: Coordinated Trading Model
        self.coordinated_trading_endpoint_config = sagemaker.CfnEndpointConfig(
            self, "CoordinatedTradingEndpointConfig",
            endpoint_config_name="fraud-demo-coordinated-trading",
            production_variants=[
                sagemaker.CfnEndpointConfig.ProductionVariantProperty(
                    variant_name="primary",
                    model_name="fraud-demo-coordinated-trading-model",
                    instance_type="ml.m5.large",
                    initial_instance_count=1,
                    initial_variant_weight=1.0,
                ),
            ],
        )

        # Endpoint Config: Login Risk Scoring
        self.login_risk_endpoint_config = sagemaker.CfnEndpointConfig(
            self, "LoginRiskEndpointConfig",
            endpoint_config_name="fraud-demo-login-risk",
            production_variants=[
                sagemaker.CfnEndpointConfig.ProductionVariantProperty(
                    variant_name="primary",
                    model_name="fraud-demo-login-risk-model",
                    instance_type="ml.m5.large",
                    initial_instance_count=1,
                    initial_variant_weight=1.0,
                ),
            ],
        )

        # Endpoint Config: Registration Anomaly / Bot Detection
        self.registration_endpoint_config = sagemaker.CfnEndpointConfig(
            self, "RegistrationEndpointConfig",
            endpoint_config_name="fraud-demo-registration-anomaly",
            production_variants=[
                sagemaker.CfnEndpointConfig.ProductionVariantProperty(
                    variant_name="primary",
                    model_name="fraud-demo-registration-anomaly-model",
                    instance_type="ml.m5.large",
                    initial_instance_count=1,
                    initial_variant_weight=1.0,
                ),
            ],
        )

        # ===== OUTPUTS =====
        CfnOutput(self, "SageMakerRoleArn", value=self.sagemaker_role.role_arn)
        CfnOutput(self, "FeatureStoreBucket",
                  value=f"s3://{ml_artifacts_bucket.bucket_name}/feature-store/")

