
from aws_cdk import (
    Stack, Duration, CfnOutput,
    aws_events as events,
    aws_events_targets as targets,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_lambda as _lambda,
    aws_iam as iam,
)
from constructs import Construct


class AlertingStack(Stack):
    """EventBridge, SNS, and Step Functions for fraud alert orchestration."""

    def __init__(self, scope: Construct, construct_id: str, config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ===== SNS TOPICS =====
        # High-priority alerts (immediate notification)
        self.critical_alerts_topic = sns.Topic(
            self, "CriticalAlertsTopic",
            topic_name="fraud-demo-critical-alerts",
            display_name="Fraud Detection - Critical Alerts",
        )

        # Standard alerts (batch/digest)
        self.standard_alerts_topic = sns.Topic(
            self, "StandardAlertsTopic",
            topic_name="fraud-demo-standard-alerts",
            display_name="Fraud Detection - Standard Alerts",
        )

        # Subscribe demo email (configurable)
        demo_email = config.get("alert_email", "fraud-demo@example.com")
        self.critical_alerts_topic.add_subscription(
            subscriptions.EmailSubscription(demo_email)
        )

        # ===== EVENTBRIDGE CUSTOM EVENT BUS =====
        self.fraud_event_bus = events.EventBus(
            self, "FraudEventBus",
            event_bus_name="fraud-detection-demo",
        )

        # Rule: Route critical alerts (high risk score)
        events.Rule(
            self, "CriticalAlertRule",
            event_bus=self.fraud_event_bus,
            rule_name="fraud-demo-critical-alerts",
            event_pattern=events.EventPattern(
                source=["fraud.detection"],
                detail_type=["FraudAlert"],
                detail={
                    "severity": ["CRITICAL"],
                },
            ),
            targets=[targets.SnsTopic(self.critical_alerts_topic)],
        )

        # Rule: Route all alerts to investigation workflow
        events.Rule(
            self, "InvestigationWorkflowRule",
            event_bus=self.fraud_event_bus,
            rule_name="fraud-demo-investigation-trigger",
            event_pattern=events.EventPattern(
                source=["fraud.detection"],
                detail_type=["FraudAlert"],
            ),
            targets=[],  # Step Functions target added below
        )

        # ===== STEP FUNCTIONS: INVESTIGATION WORKFLOW =====
        # Define investigation workflow states
        # Step 1: Enrich alert with entity context
        enrich_step = sfn.Pass(
            self, "EnrichAlert",
            comment="Enrich alert with entity profile from DynamoDB",
            result=sfn.Result.from_object({"status": "enriched"}),
        )

        # Step 2: Determine action based on typology + severity
        determine_action = sfn.Choice(
            self, "DetermineAction",
            comment="Route based on alert severity",
        )

        # Step 3a: Auto-block (critical)
        auto_block = sfn.Pass(
            self, "AutoBlock",
            comment="Execute automatic blocking action",
        )

        # Step 3b: Flag for review (high)
        flag_for_review = sfn.Pass(
            self, "FlagForReview",
            comment="Add to analyst review queue",
        )

        # Step 3c: Monitor (medium/low)
        monitor = sfn.Pass(
            self, "Monitor",
            comment="Add to monitoring watchlist",
        )

        # Step 4: Record decision
        record_decision = sfn.Pass(
            self, "RecordDecision",
            comment="Persist alert resolution to DynamoDB",
        )

        # Assemble workflow
        definition = (
            enrich_step
            .next(determine_action
                .when(sfn.Condition.string_equals("$.detail.severity", "CRITICAL"), auto_block)
                .when(sfn.Condition.string_equals("$.detail.severity", "HIGH"), flag_for_review)
                .otherwise(monitor))
        )

        # Wire terminal states to record
        auto_block.next(record_decision)
        flag_for_review.next(record_decision)
        monitor.next(record_decision)

        self.investigation_workflow = sfn.StateMachine(
            self, "InvestigationWorkflow",
            state_machine_name="fraud-demo-investigation",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.minutes(5),
        )

        # ===== OUTPUTS =====
        CfnOutput(self, "EventBusArn", value=self.fraud_event_bus.event_bus_arn)
        CfnOutput(self, "CriticalTopicArn", value=self.critical_alerts_topic.topic_arn)
        CfnOutput(self, "WorkflowArn", value=self.investigation_workflow.state_machine_arn)

