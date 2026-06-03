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
β"‚   β"‚   β"œβ"€β"€ coordinated_ring.py
β"‚   β"‚   β"œβ"€β"€ system_abuser.py
β"‚   β"‚   β"œβ"€β"€ abusive_registrant.py
β"‚   β"‚   └── ato_attacker.py
β"‚   β"œβ"€β"€ producers/                     ← Kafka topic producers
β"‚   β"‚   β"œβ"€β"€ trade_producer.py
β"‚   β"‚   β"œβ"€β"€ session_producer.py
β"‚   β"‚   β"œβ"€β"€ registration_producer.py
β"‚   β"‚   └── api_call_producer.py
β"‚   β"œβ"€β"€ seeders/                       ← RDS & S3 data seeders
β"‚   β"‚   β"œβ"€β"€ rds_seeder.py
β"‚   β"‚   └── s3_seeder.py
β"‚   └── utils/
β"‚       β"œβ"€β"€ geo_data.py
β"‚       β"œβ"€β"€ device_fingerprint.py
β"‚       └── kyc_generator.py
β"œβ"€β"€ flink_jobs/                        ← Flink Python/SQL jobs
β"‚   β"œβ"€β"€ coordinated_trading/
β"‚   β"‚   β"œβ"€β"€ job.py
β"‚   β"‚   └── rules.py
β"‚   β"œβ"€β"€ system_abuse/
β"‚   β"‚   β"œβ"€β"€ job.py
β"‚   β"‚   └── rules.py
β"‚   └── account_takeover/
β"‚       β"œβ"€β"€ job.py
β"‚       └── rules.py
β"œβ"€β"€ ml_models/                         ← SageMaker training notebooks & inference
β"‚   β"œβ"€β"€ coordinated_trading/
β"‚   β"‚   β"œβ"€β"€ train.py
β"‚   β"‚   └── inference.py
β"‚   β"œβ"€β"€ registration_anomaly/
β"‚   β"‚   β"œβ"€β"€ train.py
β"‚   β"‚   └── inference.py
β"‚   └── login_risk/
β"‚       β"œβ"€β"€ train.py
β"‚       └── inference.py
β"œβ"€β"€ lambda_functions/                  ← Lambda alert handlers
β"‚   β"œβ"€β"€ alert_processor/
β"‚   β"‚   β"œβ"€β"€ handler.py
β"‚   β"‚   └── requirements.txt
β"‚   └── topic_initializer/
β"‚       β"œβ"€β"€ handler.py
β"‚       └── requirements.txt
β"œβ"€β"€ opensearch/                        ← OpenSearch index templates & dashboards
β"‚   β"œβ"€β"€ index_templates/
β"‚   β"‚   β"œβ"€β"€ fraud_events.json
β"‚   β"‚   β"œβ"€β"€ fraud_alerts.json
β"‚   β"‚   └── fraud_entities.json
β"‚   └── dashboards/
β"‚       β"œβ"€β"€ fraud_overview.ndjson
β"‚       β"œβ"€β"€ coordinated_trading.ndjson
β"‚       β"œβ"€β"€ system_abuse.ndjson
β"‚       └── account_takeover.ndjson
β"œβ"€β"€ db/                                ← Database schema & seed scripts
β"‚   β"œβ"€β"€ schema.sql
β"‚   └── seed_reference_data.sql
β"œβ"€β"€ scripts/                           ← Deployment & utility scripts
β"‚   β"œβ"€β"€ deploy.sh
β"‚   β"œβ"€β"€ setup_kafka_topics.py
β"‚   β"œβ"€β"€ upload_ml_models.py
β"‚   └── setup_opensearch.py
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
