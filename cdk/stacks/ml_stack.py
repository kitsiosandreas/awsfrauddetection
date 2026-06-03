"""
ML Stack
SageMaker Feature Store and real-time inference endpoints for three fraud models:
  1. Coordinated Trading Detector (Isolation Forest + Graph features)
  2. Registration Anomaly / Bot Detector (Autoencoder)
  3. Login Risk Scorer (LSTM behavioural baseline)
"""
from aws_cdk import (
    Stack, CfnOutput,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_sagemaker as sagemaker,
    aws_s3 as s3,
)
from constructs import Construct


class MLStack(Stack):

    def __init__(self, scope: Construct, construct_id: str,
                 vpc: ec2.Vpc,
                 ml_artifacts_bucket: s3.Bucket,
                 config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        instance_type = config.get("sagemaker_instance_type", "ml.m5.large")

        # ── SageMaker Execution Role ──────────────────────────────────────────
        self.sagemaker_role = iam.Role(
            self, "SageMakerRole",
            role_name="fraud-demo-sagemaker-execution",
            assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess"),
            ],
        )
        ml_artifacts_bucket.grant_read_write(self.sagemaker_role)

        # ── SageMaker Feature Groups ──────────────────────────────────────────
        # Feature Group 1: Account Trading Profile (used by coordinated trading model)
        self.account_trading_fg = sagemaker.CfnFeatureGroup(
            self, "AccountTradingFG",
            feature_group_name="fraud-demo-account-trading-profile",
            record_identifier_feature_name="account_id",
            event_time_feature_name="event_time",
            online_store_config={"enableOnlineStore": True},
            offline_store_config={
                "s3StorageConfig": {
                    "s3Uri": f"s3://{ml_artifacts_bucket.bucket_name}/feature-store/account-trading/"
                }
            },
            role_arn=self.sagemaker_role.role_arn,
            feature_definitions=[
                {"featureName": "account_id",              "featureType": "String"},
                {"featureName": "event_time",              "featureType": "Fractional"},
                {"featureName": "avg_trade_size_7d",       "featureType": "Fractional"},
                {"featureName": "trade_count_24h",         "featureType": "Integral"},
                {"featureName": "unique_instruments_7d",   "featureType": "Integral"},
                {"featureName": "avg_hold_duration_min",   "featureType": "Fractional"},
                {"featureName": "pnl_volatility_7d",       "featureType": "Fractional"},
                {"featureName": "peak_trading_hour",       "featureType": "Integral"},
                {"featureName": "instrument_concentration","featureType": "Fractional"},
                {"featureName": "correlated_accounts_cnt", "featureType": "Integral"},
                {"featureName": "cluster_id",              "featureType": "String"},
                {"featureName": "win_rate_7d",             "featureType": "Fractional"},
                {"featureName": "trade_timing_entropy",    "featureType": "Fractional"},
            ],
        )

        # Feature Group 2: Login Behaviour Profile (used by ATO model)
        self.login_behavior_fg = sagemaker.CfnFeatureGroup(
            self, "LoginBehaviorFG",
            feature_group_name="fraud-demo-login-behavior",
            record_identifier_feature_name="account_id",
            event_time_feature_name="event_time",
            online_store_config={"enableOnlineStore": True},
            role_arn=self.sagemaker_role.role_arn,
            feature_definitions=[
                {"featureName": "account_id",             "featureType": "String"},
                {"featureName": "event_time",             "featureType": "Fractional"},
                {"featureName": "known_device_count",     "featureType": "Integral"},
                {"featureName": "known_ip_count",         "featureType": "Integral"},
                {"featureName": "avg_session_duration_min","featureType": "Fractional"},
                {"featureName": "login_hour_mode",        "featureType": "Integral"},
                {"featureName": "country_code_primary",   "featureType": "String"},
                {"featureName": "failed_logins_7d",       "featureType": "Integral"},
                {"featureName": "pw_reset_count_30d",     "featureType": "Integral"},
                {"featureName": "geo_velocity_max_kph",   "featureType": "Fractional"},
            ],
        )

        # Feature Group 3: Registration Profile (used by bot detection model)
        self.registration_fg = sagemaker.CfnFeatureGroup(
            self, "RegistrationFG",
            feature_group_name="fraud-demo-registration-profile",
            record_identifier_feature_name="account_id",
            event_time_feature_name="event_time",
            online_store_config={"enableOnlineStore": True},
            role_arn=self.sagemaker_role.role_arn,
            feature_definitions=[
                {"featureName": "account_id",             "featureType": "String"},
                {"featureName": "event_time",             "featureType": "Fractional"},
                {"featureName": "ip_registration_count",  "featureType": "Integral"},
                {"featureName": "device_fp_count",        "featureType": "Integral"},
                {"featureName": "kyc_similarity_score",   "featureType": "Fractional"},
                {"featureName": "registration_hour",      "featureType": "Integral"},
                {"featureName": "form_fill_time_sec",     "featureType": "Fractional"},
                {"featureName": "email_domain_risk",      "featureType": "Fractional"},
                {"featureName": "api_rate_per_min",       "featureType": "Fractional"},
                {"featureName": "order_cancel_ratio",     "featureType": "Fractional"},
            ],
        )

        # ── SageMaker Models ──────────────────────────────────────────────────
        # NOTE: Model artifacts (model.tar.gz) are uploaded by scripts/upload_ml_models.py
        # before CDK deploy. The ECR image is the SageMaker sklearn container.
        sklearn_image = (
            f"683313688378.dkr.ecr.{self.region}.amazonaws.com/"
            f"sagemaker-scikit-learn:1.2-1-cpu-py3"
        )

        self.coord_trading_model = sagemaker.CfnModel(
            self, "CoordTradingModel",
            model_name="fraud-demo-coordinated-trading-model",
            execution_role_arn=self.sagemaker_role.role_arn,
            primary_container=sagemaker.CfnModel.ContainerDefinitionProperty(
                image=sklearn_image,
                model_data_url=f"s3://{ml_artifacts_bucket.bucket_name}/models/coordinated_trading/model.tar.gz",
                environment={"SAGEMAKER_PROGRAM": "inference.py"},
            ),
            vpc_config=sagemaker.CfnModel.VpcConfigProperty(
                subnets=[s.subnet_id for s in vpc.private_subnets[:2]],
                security_group_ids=[],
            ),
        )

        self.login_risk_model = sagemaker.CfnModel(
            self, "LoginRiskModel",
            model_name="fraud-demo-login-risk-model",
            execution_role_arn=self.sagemaker_role.role_arn,
            primary_container=sagemaker.CfnModel.ContainerDefinitionProperty(
                image=sklearn_image,
                model_data_url=f"s3://{ml_artifacts_bucket.bucket_name}/models/login_risk/model.tar.gz",
                environment={"SAGEMAKER_PROGRAM": "inference.py"},
            ),
        )

        self.registration_model = sagemaker.CfnModel(
            self, "RegistrationModel",
            model_name="fraud-demo-registration-anomaly-model",
            execution_role_arn=self.sagemaker_role.role_arn,
            primary_container=sagemaker.CfnModel.ContainerDefinitionProperty(
                image=sklearn_image,
                model_data_url=f"s3://{ml_artifacts_bucket.bucket_name}/models/registration_anomaly/model.tar.gz",
                environment={"SAGEMAKER_PROGRAM": "inference.py"},
            ),
        )

        # ── Endpoint Configs ──────────────────────────────────────────────────
        def _ep_config(name, model_name):
            return sagemaker.CfnEndpointConfig(
                self, f"{name}EpConfig",
                endpoint_config_name=f"fraud-demo-{name}-config",
                production_variants=[
                    sagemaker.CfnEndpointConfig.ProductionVariantProperty(
                        variant_name="primary",
                        model_name=model_name,
                        instance_type=instance_type,
                        initial_instance_count=1,
                        initial_variant_weight=1.0,
                    )
                ],
            )

        coord_ep_cfg = _ep_config("coordinated-trading", "fraud-demo-coordinated-trading-model")
        login_ep_cfg = _ep_config("login-risk", "fraud-demo-login-risk-model")
        reg_ep_cfg   = _ep_config("registration-anomaly", "fraud-demo-registration-anomaly-model")

        coord_ep_cfg.add_dependency(self.coord_trading_model)
        login_ep_cfg.add_dependency(self.login_risk_model)
        reg_ep_cfg.add_dependency(self.registration_model)

        # ── Endpoints ────────────────────────────────────────────────────────
        self.coord_endpoint = sagemaker.CfnEndpoint(
            self, "CoordTradingEndpoint",
            endpoint_name="fraud-demo-coordinated-trading",
            endpoint_config_name=coord_ep_cfg.endpoint_config_name,
        )
        self.login_endpoint = sagemaker.CfnEndpoint(
            self, "LoginRiskEndpoint",
            endpoint_name="fraud-demo-login-risk",
            endpoint_config_name=login_ep_cfg.endpoint_config_name,
        )
        self.registration_endpoint = sagemaker.CfnEndpoint(
            self, "RegistrationEndpoint",
            endpoint_name="fraud-demo-registration-anomaly",
            endpoint_config_name=reg_ep_cfg.endpoint_config_name,
        )

        # ── Outputs ──────────────────────────────────────────────────────────
        CfnOutput(self, "SageMakerRoleArn",
                  value=self.sagemaker_role.role_arn,
                  export_name="FraudDemo-SageMakerRoleArn")
        CfnOutput(self, "CoordTradingEndpoint",
                  value=self.coord_endpoint.endpoint_name or "fraud-demo-coordinated-trading",
                  export_name="FraudDemo-CoordTradingEndpoint")
        CfnOutput(self, "LoginRiskEndpoint",
                  value=self.login_endpoint.endpoint_name or "fraud-demo-login-risk",
                  export_name="FraudDemo-LoginRiskEndpoint")
