"""
Alerting Stack
EventBridge custom bus, SNS topics, Lambda alert processor,
and Step Functions investigation workflow.
"""
from aws_cdk import (
    Stack, Duration, RemovalPolicy, CfnOutput,
    aws_events as events,
    aws_events_targets as targets,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_stepfunctions as sfn,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_logs as logs,
    aws_ec2 as ec2,
)
from constructs import Construct


class AlertingStack(Stack):

    def __init__(self, scope: Construct, construct_id: str,
                 vpc: ec2.Vpc,
                 lambda_sg: ec2.SecurityGroup,
                 alerts_table_name: str,
                 opensearch_endpoint: str,
                 opensearch_domain_arn: str,
                 event_bus_arn,  # None — created here
                 config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        alert_email = config.get("alert_email", "fraud-demo-alerts@example.com")

        # ── SNS Topics ────────────────────────────────────────────────────────
        self.critical_topic = sns.Topic(
            self, "CriticalAlertsTopic",
            topic_name="fraud-demo-critical-alerts",
            display_name="Fraud Detection — Critical Alerts",
        )
        self.standard_topic = sns.Topic(
            self, "StandardAlertsTopic",
            topic_name="fraud-demo-standard-alerts",
            display_name="Fraud Detection — Standard Alerts",
        )
        self.critical_topic.add_subscription(subscriptions.EmailSubscription(alert_email))

        # ── EventBridge Custom Bus ────────────────────────────────────────────
        self.fraud_bus = events.EventBus(
            self, "FraudEventBus",
            event_bus_name="fraud-detection-demo",
        )

        # ── Lambda: Alert Processor ───────────────────────────────────────────
        alert_role = iam.Role(
            self, "AlertProcessorRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaVPCAccessExecutionRole"
                ),
            ],
        )
        alert_role.add_to_policy(iam.PolicyStatement(
            actions=["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:GetItem"],
            resources=[f"arn:aws:dynamodb:{self.region}:{self.account}:table/{alerts_table_name}*"],
        ))
        alert_role.add_to_policy(iam.PolicyStatement(
            actions=["es:ESHttpPost", "es:ESHttpPut"],
            resources=[f"{opensearch_domain_arn}/*"],
        ))
        alert_role.add_to_policy(iam.PolicyStatement(
            actions=["sns:Publish"],
            resources=[self.critical_topic.topic_arn, self.standard_topic.topic_arn],
        ))
        alert_role.add_to_policy(iam.PolicyStatement(
            actions=["events:PutEvents"],
            resources=[self.fraud_bus.event_bus_arn],
        ))

        self.alert_processor = _lambda.Function(
            self, "AlertProcessor",
            function_name="fraud-demo-alert-processor",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="handler.handler",
            code=_lambda.Code.from_asset("../lambda_functions/alert_processor"),
            role=alert_role,
            timeout=Duration.seconds(30),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            security_groups=[lambda_sg],
            environment={
                "ALERTS_TABLE": alerts_table_name,
                "OPENSEARCH_ENDPOINT": opensearch_endpoint,
                "CRITICAL_TOPIC_ARN": self.critical_topic.topic_arn,
                "STANDARD_TOPIC_ARN": self.standard_topic.topic_arn,
                "EVENT_BUS_NAME": "fraud-detection-demo",
            },
            log_retention=logs.RetentionDays.ONE_WEEK,
        )

        # ── Step Functions: Investigation Workflow ────────────────────────────
        # State 1 — Enrich
        enrich = sfn.Pass(
            self, "EnrichAlert",
            comment="Enrich alert with entity context from DynamoDB",
            result_path="$.enrichment",
        )

        # State 2 — Score severity gate
        critical_gate = sfn.Choice(self, "SeverityGate")

        # Terminal states
        auto_block = sfn.Pass(self, "AutoBlock",
                              comment="Suspend account — await manual review")
        flag_review = sfn.Pass(self, "FlagForReview",
                               comment="Queue for analyst review")
        monitor = sfn.Pass(self, "MonitorWatchlist",
                           comment="Add to enhanced monitoring")
        record = sfn.Pass(self, "RecordDecision",
                          comment="Persist resolution to DynamoDB")

        # Wire
        auto_block.next(record)
        flag_review.next(record)
        monitor.next(record)

        definition = enrich.next(
            critical_gate
            .when(sfn.Condition.string_equals("$.severity", "CRITICAL"), auto_block)
            .when(sfn.Condition.string_equals("$.severity", "HIGH"), flag_review)
            .otherwise(monitor)
        )

        self.investigation_sm = sfn.StateMachine(
            self, "InvestigationWorkflow",
            state_machine_name="fraud-demo-investigation",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.minutes(5),
            logs=sfn.LogOptions(
                destination=logs.LogGroup(
                    self, "SfnLogGroup",
                    log_group_name="/aws/states/fraud-demo-investigation",
                    removal_policy=RemovalPolicy.DESTROY,
                    retention=logs.RetentionDays.ONE_WEEK,
                ),
                level=sfn.LogLevel.ERROR,
            ),
        )

        # ── EventBridge Rules ─────────────────────────────────────────────────
        # CRITICAL → SNS
        events.Rule(
            self, "CriticalAlertRule",
            event_bus=self.fraud_bus,
            rule_name="fraud-demo-critical-to-sns",
            event_pattern=events.EventPattern(
                source=["fraud.detection"],
                detail_type=["FraudAlert"],
                detail={"severity": ["CRITICAL"]},
            ),
            targets=[targets.SnsTopic(self.critical_topic)],
        )

        # ALL alerts → Lambda processor (writes to DynamoDB + OpenSearch)
        events.Rule(
            self, "AllAlertsToLambda",
            event_bus=self.fraud_bus,
            rule_name="fraud-demo-all-alerts-processor",
            event_pattern=events.EventPattern(
                source=["fraud.detection"],
                detail_type=["FraudAlert"],
            ),
            targets=[targets.LambdaFunction(self.alert_processor)],
        )

        # ALL alerts → Step Functions investigation
        events.Rule(
            self, "AllAlertsToSfn",
            event_bus=self.fraud_bus,
            rule_name="fraud-demo-all-alerts-investigation",
            event_pattern=events.EventPattern(
                source=["fraud.detection"],
                detail_type=["FraudAlert"],
            ),
            targets=[targets.SfnStateMachine(self.investigation_sm)],
        )

        # ── Outputs ──────────────────────────────────────────────────────────
        CfnOutput(self, "EventBusArn",
                  value=self.fraud_bus.event_bus_arn,
                  export_name="FraudDemo-EventBusArn")
        CfnOutput(self, "CriticalTopicArn",
                  value=self.critical_topic.topic_arn,
                  export_name="FraudDemo-CriticalTopicArn")
        CfnOutput(self, "InvestigationWorkflowArn",
                  value=self.investigation_sm.state_machine_arn,
                  export_name="FraudDemo-InvestigationWorkflowArn")
