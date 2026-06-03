# Forex/CFD Fraud Detection Demo

Real-time fraud detection platform combining rule-based and ML-based detection
across three typologies: Coordinated Trading, System Abuse & Abusive Registrations,
and Account Takeover (ATO).

## Architecture

```
Data Generator (ECS Fargate)
    - MSK (Kafka) — 7 topics
    - Amazon Managed Flink — 3 detection apps
    - SageMaker Endpoints — 3 ML models
    - Neptune — account graph
    - OpenSearch — dashboards & alerting
    - DynamoDB — alert store & velocity counters
    - RDS PostgreSQL — account master data
    - SNS + EventBridge — alert fan-out
```

## Project Structure

```
fraud-detection-demo/
β"œβ"€β"€ README.md                          ← This file
β"œβ"€β"€ cdk/                               ← CDK infrastructure (Python)
β"‚   β"œβ"€β"€ app.py                         ← CDK app entry point
β"‚   β"œβ"€β"€ cdk.json
β"‚   β"œβ"€β"€ requirements.txt
β"‚   └── stacks/
β"‚       β"œβ"€β"€ networking_stack.py
β"‚       β"œβ"€β"€ data_storage_stack.py
β"‚       β"œβ"€β"€ streaming_stack.py
β"‚       β"œβ"€β"€ graph_stack.py
β"‚       β"œβ"€β"€ search_stack.py
β"‚       β"œβ"€β"€ ml_stack.py
β"‚       β"œβ"€β"€ processing_stack.py
β"‚       β"œβ"€β"€ alerting_stack.py
β"‚       └── compute_stack.py
β"œβ"€β"€ data_generator/                    ← Synthetic data generator (Python)
β"‚   β"œβ"€β"€ Dockerfile
β"‚   β"œβ"€β"€ requirements.txt
β"‚   β"œβ"€β"€ main.py                        ← Entry point
β"‚   β"œβ"€β"€ config.py                      ← Runtime configuration
β"‚   β"œβ"€β"€ personas/                      ← Fraud persona implementations
β"‚   β"‚   β"œβ"€β"€ base.py
β"‚   β"‚   β"œβ"€β"€ normal_trader.py
fraud-detection-demo/
 README.md                          ← This file
 cdk/                               ← CDK infrastructure (Python)
-    app.py                         ← CDK app entry point
-    cdk.json
-    requirements.txt
-   └── stacks/
-        networking_stack.py
-        data_storage_stack.py
-        streaming_stack.py
-        graph_stack.py
-        search_stack.py
-        ml_stack.py
-        processing_stack.py
-        alerting_stack.py
-       └── compute_stack.py
 data_generator/                    ← Synthetic data generator (Python)
-    Dockerfile
-    requirements.txt
-    main.py                        ← Entry point
-    config.py                      ← Runtime configuration
-    personas/                      ← Fraud persona implementations
-   -    base.py
-   -    normal_trader.py
-   -    coordinated_ring.py
-   -    system_abuser.py
-   -    abusive_registrant.py
-   -   └── ato_attacker.py
-    producers/                     ← Kafka topic producers
-   -    trade_producer.py
-   -    session_producer.py
-   -    registration_producer.py
-   -   └── api_call_producer.py
-    seeders/                       ← RDS & S3 data seeders
-   -    rds_seeder.py
-   -   └── s3_seeder.py
-   └── utils/
-        geo_data.py
-        device_fingerprint.py
-       └── kyc_generator.py
 flink_jobs/                        ← Flink Python/SQL jobs
-    coordinated_trading/
-   -    job.py
-   -   └── rules.py
-    system_abuse/
-   -    job.py
-   -   └── rules.py
-   └── account_takeover/
-        job.py
-       └── rules.py
 ml_models/                         ← SageMaker training notebooks & inference
-    coordinated_trading/
-   -    train.py
-   -   └── inference.py
-    registration_anomaly/
-   -    train.py
-   -   └── inference.py
-   └── login_risk/
-        train.py
-       └── inference.py
 lambda_functions/                  ← Lambda alert handlers
-    alert_processor/
-   -    handler.py
-   -   └── requirements.txt
-   └── topic_initializer/
-        handler.py
-       └── requirements.txt
 opensearch/                        ← OpenSearch index templates & dashboards
-    index_templates/
-   -    fraud_events.json
-   -    fraud_alerts.json
-   -   └── fraud_entities.json
-   └── dashboards/
-        fraud_overview.ndjson
-        coordinated_trading.ndjson
-        system_abuse.ndjson
-       └── account_takeover.ndjson
 db/                                ← Database schema & seed scripts
-    schema.sql
-   └── seed_reference_data.sql
 scripts/                           ← Deployment & utility scripts
-    deploy.sh
-    setup_kafka_topics.py
-    upload_ml_models.py
-   └── setup_opensearch.py
└── docs/
    └── demo_runbook.md                ← Step-by-step presenter guide
```

## Prerequisites

- AWS CLI configured with sufficient IAM permissions
- AWS CDK v2 (`npm install -g aws-cdk`)
- Python 3.11+
- Docker (for building data generator image)
- Node.js 18+ (for CDK)

## Deployment Steps

### 1. Bootstrap CDK (first time only)
```bash
cd cdk
pip install -r requirements.txt
cdk bootstrap
```

### 2. Deploy infrastructure
```bash
cd cdk
cdk deploy --all --require-approval never \
  --parameters RdsMasterPassword=YourSecurePassword123! \
  --parameters AlertEmail=your-email@example.com
```

### 3. Set up Kafka topics
```bash
MSK_CLUSTER_ARN=$(aws cloudformation describe-stacks \
  --stack-name FraudDemo-Streaming \
  --query "Stacks[0].Outputs[?OutputKey=='MskClusterArn'].OutputValue" \
  --output text)

python scripts/setup_kafka_topics.py --cluster-arn $MSK_CLUSTER_ARN
```

### 4. Set up OpenSearch indices and dashboards
```bash
OPENSEARCH_ENDPOINT=$(aws cloudformation describe-stacks \
  --stack-name FraudDemo-Search \
  --query "Stacks[0].Outputs[?OutputKey=='OpenSearchEndpoint'].OutputValue" \
  --output text)

python scripts/setup_opensearch.py --endpoint $OPENSEARCH_ENDPOINT
```

### 5. Train and upload ML models
```bash
python scripts/upload_ml_models.py --region us-east-1
```

### 6. Start data generator
```bash
# Via ECS (production demo)
aws ecs run-task \
  --cluster fraud-demo-datagen \
  --task-definition fraud-demo-datagen \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[...],securityGroups=[...],assignPublicIp=DISABLED}"

# Or locally for testing
cd data_generator
pip install -r requirements.txt
python main.py --scenario mixed --tps 100 --accounts 500
```

### 7. Start Flink applications
```bash
aws kinesisanalyticsv2 start-application \
  --application-name fraud-demo-coordinated-trading \
  --run-configuration '{}'

aws kinesisanalyticsv2 start-application \
  --application-name fraud-demo-system-abuse \
  --run-configuration '{}'

aws kinesisanalyticsv2 start-application \
  --application-name fraud-demo-account-takeover \
  --run-configuration '{}'
```

## Demo Scenarios

See [docs/demo_runbook.md](docs/demo_runbook.md) for the full presenter guide.

| Scenario | Generator flag | What to show |
|----------|---------------|--------------|
| Baseline | `--scenario normal` | Clean dashboard, no alerts |
| System Abuse | `--scenario registration_burst` | Rule-based alerts firing in seconds |
| ATO | `--scenario ato_attack` | Geo-velocity alert + ML behavioural score |
| Coordinated Ring | `--scenario coordinated_ring` | Rules pass, ML graph clustering fires |
| All typologies | `--scenario mixed` | Full dashboard demo |

## Tear Down

```bash
cd cdk
cdk destroy --all
```

> **Note:** S3 buckets and DynamoDB tables use `RemovalPolicy.DESTROY`. All data will be deleted on stack destruction.
