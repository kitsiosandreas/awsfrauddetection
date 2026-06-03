# Forex/CFD Fraud Detection Demo — Deployment Guide

**Version:** 1.0  
**Last updated:** June 2026  
**Audience:** AWS SA / deployment engineer  
**Estimated deployment time:** 45–60 minutes (excluding MSK cluster creation ~15 min)  
**Estimated AWS cost while running:** ~$10–16 / hour  

---

## Table of Contents

1. [Overview and Architecture](#1-overview-and-architecture)
2. [Prerequisites](#2-prerequisites)
3. [Repository Structure](#3-repository-structure)
4. [IAM Permissions Required](#4-iam-permissions-required)
5. [Step 1 — Clone and configure the repository](#step-1--clone-and-configure-the-repository)
6. [Step 2 — Install local tooling](#step-2--install-local-tooling)
7. [Step 3 — Train and upload ML model artifacts](#step-3--train-and-upload-ml-model-artifacts)
8. [Step 4 — CDK bootstrap](#step-4--cdk-bootstrap)
9. [Step 5 — Deploy all CDK stacks](#step-5--deploy-all-cdk-stacks)
10. [Step 6 — Verify stack outputs](#step-6--verify-stack-outputs)
11. [Step 7 — Confirm SNS email subscription](#step-7--confirm-sns-email-subscription)
12. [Step 8 — Set up OpenSearch indices and dashboards](#step-8--set-up-opensearch-indices-and-dashboards)
13. [Step 9 — Start the Flink detection applications](#step-9--start-the-flink-detection-applications)
14. [Step 10 — Verify the data generator is running](#step-10--verify-the-data-generator-is-running)
15. [Step 11 — End-to-end smoke test](#step-11--end-to-end-smoke-test)
16. [Stack-by-stack resource reference](#stack-by-stack-resource-reference)
17. [CDK context parameters reference](#cdk-context-parameters-reference)
18. [Changing the demo scenario at runtime](#changing-the-demo-scenario-at-runtime)
19. [Monitoring and observability](#monitoring-and-observability)
20. [Cost breakdown](#cost-breakdown)
21. [Troubleshooting](#troubleshooting)
22. [Tear-down](#tear-down)

---

## 1. Overview and Architecture

The solution deploys **nine CDK stacks** in strict dependency order:

```
FraudDemo-Networking   (VPC, subnets, security groups)
       |
FraudDemo-Storage      (RDS PostgreSQL, S3 x4, DynamoDB x4)
       |
FraudDemo-Streaming    (MSK Kafka cluster, topic init Lambda)
   |       |
FraudDemo-Graph        FraudDemo-Search      FraudDemo-ML
(Neptune)              (OpenSearch)          (SageMaker)
       \         |         /
        FraudDemo-Processing  (3 x Managed Flink apps)
               |
        FraudDemo-Alerting   (EventBridge, SNS, Lambda, Step Functions)
               |
        FraudDemo-Compute    (ECS Fargate data generator)
```

**Data flow at runtime:**

```
ECS Fargate (data generator)
  --> MSK topics: trades.raw / sessions.raw / registrations.raw / api.calls
        --> Managed Flink (3 detection apps)
              --> SageMaker endpoints (ML scoring)
              --> Neptune (graph clustering)
              --> DynamoDB (velocity counters, entity state)
              --> MSK: alerts.fraud
                    --> Lambda (alert processor)
                          --> DynamoDB (alerts table)
                          --> OpenSearch (indexing)
                          --> SNS (email notification)
                    --> Step Functions (investigation workflow)
```

---

## 2. Prerequisites

### 2.1 AWS Account requirements

| Requirement | Detail |
|-------------|--------|
| Account type | Internal AWS account (not a customer account) |
| Region | Any region with all services available — **us-east-1 recommended** |
| Service limits | Ensure the following are not at limit: VPCs (need 1), EIPs (need 2), Managed Flink KPUs (need 8), Neptune instances (need 1), MSK brokers (need 3), SageMaker endpoints (need 3), OpenSearch data nodes (need 2) |

### 2.2 Local machine requirements

| Tool | Required version | Install command |
|------|-----------------|-----------------|
| Python | 3.11 or 3.12 | https://python.org/downloads |
| pip | Latest | `python -m pip install --upgrade pip` |
| Node.js | 18.x or 20.x | https://nodejs.org |
| AWS CDK CLI | 2.130.0 | `npm install -g aws-cdk@2.130.0` |
| AWS CLI | v2.x | https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html |
| Docker | 24.x+ | https://docs.docker.com/get-docker/ |
| Git | Any | https://git-scm.com |

> **Docker must be running** when you execute `cdk deploy` — CDK builds and pushes the data generator container image during the Compute stack deployment.

### 2.3 AWS CLI authentication

The deployment uses the IAM identity configured in your AWS CLI profile. Verify it before starting:

```bash
aws sts get-caller-identity
```

Expected output (values will differ):
```json
{
    "UserId": "AROAXXXXXXXXXXXXXXXXX:your-session",
    "Account": "123456789012",
    "Arn": "arn:aws:iam::123456789012:assumed-role/YourRole/your-session"
}
```

Save your account ID — you will need it in several steps:
```bash
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AWS_REGION=us-east-1   # change if deploying to a different region
echo "Account: $AWS_ACCOUNT_ID  Region: $AWS_REGION"
```

---

## 3. Repository Structure

```
fraud-detection-demo/
├── cdk/                        CDK app (Python)
│   ├── app.py                  Entry point — instantiates all 9 stacks
│   ├── cdk.json                CDK configuration
│   ├── requirements.txt        CDK Python dependencies
│   └── stacks/
│       ├── networking_stack.py
│       ├── data_storage_stack.py
│       ├── streaming_stack.py
│       ├── graph_stack.py
│       ├── search_stack.py
│       ├── ml_stack.py
│       ├── processing_stack.py
│       ├── alerting_stack.py
│       └── compute_stack.py
├── data_generator/             Synthetic event generator (Docker image)
│   ├── Dockerfile
│   ├── main.py
│   ├── config.py
│   ├── personas/               5 fraud persona implementations
│   ├── producers/              Kafka producer wrapper (MSK IAM auth)
│   ├── seeders/                RDS + S3 seed scripts
│   └── utils/                  KYC, device fingerprint, geo utilities
├── flink_jobs/                 PyFlink job logic + rule functions
│   ├── account_takeover/
│   ├── coordinated_trading/
│   └── system_abuse/
├── lambda_functions/
│   ├── alert_processor/        EventBridge → DynamoDB + OpenSearch + SNS
│   └── topic_initializer/      CloudFormation custom resource — creates Kafka topics
├── ml_models/                  Training + inference scripts (SageMaker sklearn)
│   ├── coordinated_trading/
│   ├── login_risk/
│   └── registration_anomaly/
├── opensearch/
│   └── index_templates/        Index mappings for fraud-events and fraud-alerts
├── db/
│   └── schema.sql              PostgreSQL DDL
├── scripts/
│   ├── deploy.sh               Full end-to-end deployment script
│   ├── upload_ml_models.py     Train models locally + upload to S3
│   └── setup_opensearch.py     Create index templates post-deploy
└── docs/
    ├── deployment_guide.md     This file
    └── demo_runbook.md         Presenter script
```

---

## 4. IAM Permissions Required

The IAM principal (role or user) running the deployment needs the following service permissions. The easiest approach for an internal demo account is `AdministratorAccess`. If you need a scoped policy, the minimum services required are:

```
cloudformation:*
ec2:*
iam:*
s3:*
rds:*
kafka:*
kafka-cluster:*
kinesisanalyticsv2:*
neptune-db:*
es:*
sagemaker:*
dynamodb:*
lambda:*
events:*
sns:*
states:*
ecs:*
ecr:*
logs:*
secretsmanager:*
ssm:*
sts:AssumeRole
```

Also ensure the deploying role has `iam:PassRole` — CDK creates service roles and passes them to services.

---

---

## Step 1 — Clone and configure the repository

### 1.1 Clone the repository

If the project is hosted in a Git repository, clone it to your local machine:

```bash
git clone https://github.com/your-org/fraud-detection-demo.git
cd fraud-detection-demo
```

If you are working from a local folder that already contains the files (as delivered), simply navigate to the root of the project:

```bash
cd "fraud-detection-demo"
```

Confirm you are in the right place — you should see these top-level items:

```
cdk/   data_generator/   flink_jobs/   lambda_functions/   ml_models/
opensearch/   db/   scripts/   docs/   README.md
```

### 1.2 Set environment variables

These variables are referenced throughout this guide. Set them in your shell before proceeding:

```bash
# Your AWS account ID (retrieved automatically)
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Target deployment region — us-east-1 is recommended
export AWS_REGION=us-east-1

# The email address that will receive fraud alert notifications.
# You MUST confirm the SNS subscription email that arrives in your inbox (see Step 7).
export ALERT_EMAIL="your-name@example.com"

# RDS master password — minimum 12 characters, must include letters and numbers.
# Store this securely. It will be written to Secrets Manager automatically.
export RDS_PASSWORD="FraudDemo2024!Secure"

# Verify
echo "Account: $AWS_ACCOUNT_ID | Region: $AWS_REGION"
```

> **Windows PowerShell users:** Replace `export VAR=value` with `$env:VAR = "value"` throughout this guide.
> Example: `$env:AWS_REGION = "us-east-1"`

---

## Step 2 — Install local tooling

### 2.1 Verify Python version

The CDK app, data generator, and ML training scripts all require Python 3.11 or 3.12.

```bash
python --version
# Expected: Python 3.11.x or Python 3.12.x

# If python points to Python 2, try:
python3 --version
```

### 2.2 Install Node.js and AWS CDK CLI

The CDK CLI is a Node.js package. Install the exact version used during development to avoid synthesis compatibility issues:

```bash
# Verify Node.js
node --version    # must be 18.x or 20.x
npm --version     # must be 9.x or 10.x

# Install CDK CLI globally at the pinned version
npm install -g aws-cdk@2.130.0

# Verify
cdk --version
# Expected: 2.130.0 (build xxxxxxx)
```

### 2.3 Install CDK Python dependencies

The CDK app itself is written in Python. Install its dependencies into a virtual environment:

```bash
cd cdk

# Create and activate a virtual environment
python -m venv .venv

# Linux / macOS:
source .venv/bin/activate

# Windows PowerShell:
# .\.venv\Scripts\Activate.ps1

# Install dependencies (pinned versions)
pip install -r requirements.txt
```

The `requirements.txt` installs:
- `aws-cdk-lib==2.130.0` — the core CDK constructs library
- `constructs>=10.0.0,<11.0.0` — CDK dependency
- `aws-cdk.aws-neptune-alpha==2.130.0a0` — L2 constructs for Neptune

After installation, verify CDK can synthesize the app without errors:

```bash
cdk synth --quiet
```

You should see output beginning with `Successfully synthesized to cdk.out`. If you see import errors, ensure your virtual environment is active.

```bash
cd ..    # return to repo root
```

### 2.4 Install Python dependencies for scripts

The utility scripts (ML training, OpenSearch setup) have their own dependencies:

```bash
# From the repo root
pip install \
  scikit-learn==1.4.1 \
  numpy==1.26.4 \
  boto3==1.34.69 \
  faker==24.3.0 \
  psycopg2-binary==2.9.9
```

### 2.5 Verify Docker is running

Docker is required for `cdk deploy` to build and push the data generator container image to ECR. Run:

```bash
docker info
```

You should see Docker engine information. If you see `Cannot connect to the Docker daemon`, start Docker Desktop and retry before proceeding.

---

## Step 3 — Train and upload ML model artifacts

**Why this must happen before CDK deploy:** The `FraudDemo-ML` stack creates SageMaker models that reference `model.tar.gz` files in S3. If those files do not exist when CloudFormation creates the model resources, the stack will fail. The `upload_ml_models.py` script trains the models locally (takes 1–3 minutes per model) and uploads the artifacts to the correct S3 paths.

### 3.1 Understanding what gets trained

Three scikit-learn models are trained using synthetically generated labelled data:

| Model | Algorithm | S3 path |
|-------|-----------|---------|
| Coordinated Trading Detector | Isolation Forest | `models/coordinated_trading/model.tar.gz` |
| Registration Anomaly / Bot Detector | Autoencoder (MLPRegressor) | `models/registration_anomaly/model.tar.gz` |
| Login Risk Scorer | Random Forest Classifier | `models/login_risk/model.tar.gz` |

Each model artifact (`model.tar.gz`) contains:
- `model.pkl` — the serialised fitted model
- `model_meta.json` — training metadata (features, thresholds, evaluation metrics)
- `inference.py` — the SageMaker inference entry point

### 3.2 First-time bootstrap: create the ML artifacts bucket

The upload script needs the ML artifacts S3 bucket to already exist. However, because the Storage stack hasn't been deployed yet, you need to create the bucket manually first, **or** deploy just the Storage stack before training.

**Option A (recommended): Deploy Storage stack first, then train**

```bash
cd cdk
source .venv/bin/activate    # if not already active

cdk deploy FraudDemo-Networking FraudDemo-Storage \
  --require-approval never \
  --context region=$AWS_REGION \
  --context account=$AWS_ACCOUNT_ID \
  --context alertEmail=$ALERT_EMAIL
```

Wait for both stacks to reach `CREATE_COMPLETE` (approximately 8–12 minutes for RDS).

Then get the ML artifacts bucket name from the stack output:

```bash
export ML_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name FraudDemo-Storage \
  --region $AWS_REGION \
  --query "Stacks[0].Outputs[?OutputKey=='MlArtifactsBucket'].OutputValue" \
  --output text)

echo "ML bucket: $ML_BUCKET"
# Expected: fraud-demo-ml-123456789012-us-east-1
```

**Option B (quick): Create bucket manually**

```bash
export ML_BUCKET="fraud-demo-ml-${AWS_ACCOUNT_ID}-${AWS_REGION}"

aws s3 mb s3://$ML_BUCKET --region $AWS_REGION

aws s3api put-bucket-encryption \
  --bucket $ML_BUCKET \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

aws s3api put-public-access-block \
  --bucket $ML_BUCKET \
  --public-access-block-configuration \
  "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
```

### 3.3 Run the training and upload script

```bash
# From the repo root
python scripts/upload_ml_models.py \
  --region $AWS_REGION \
  --bucket $ML_BUCKET
```

The script will:
1. Run `ml_models/coordinated_trading/train.py` — trains Isolation Forest, packages artifact
2. Run `ml_models/registration_anomaly/train.py` — trains Autoencoder, packages artifact
3. Run `ml_models/login_risk/train.py` — trains Random Forest, packages artifact
4. Upload all three `model.tar.gz` files to S3

Expected output:
```
[INFO] Training ml_models/coordinated_trading/train.py...
[INFO] Training recall on fraud: 0.923
[INFO] Saved to /tmp/.../model.tar.gz
[INFO] Uploading ... → s3://fraud-demo-ml-.../models/coordinated_trading/model.tar.gz

[INFO] Training ml_models/registration_anomaly/train.py...
[INFO] Threshold: 0.0214 | Fraud recall: 0.961
[INFO] Saved to /tmp/.../model.tar.gz
[INFO] Uploading ... → s3://fraud-demo-ml-.../models/registration_anomaly/model.tar.gz

[INFO] Training ml_models/login_risk/train.py...
[INFO] CV AUC: 0.987 ± 0.003
[INFO] Saved to /tmp/.../model.tar.gz
[INFO] Uploading ... → s3://fraud-demo-ml-.../models/login_risk/model.tar.gz

[INFO] All models uploaded. You can now run 'cdk deploy FraudDemo-ML'.
```

### 3.4 Verify the artifacts landed in S3

```bash
aws s3 ls s3://$ML_BUCKET/models/ --recursive
```

You should see three `model.tar.gz` files:
```
2026-06-03  models/coordinated_trading/model.tar.gz
2026-06-03  models/login_risk/model.tar.gz
2026-06-03  models/registration_anomaly/model.tar.gz
```

If any are missing, re-run the upload script for the specific model:
```bash
python scripts/upload_ml_models.py --region $AWS_REGION --bucket $ML_BUCKET
```

---

## Step 4 — CDK Bootstrap

CDK Bootstrap provisions the resources that CDK needs to deploy assets (S3 bucket for CloudFormation templates, ECR repository for Docker images, IAM roles for deployments). This only needs to be run once per account/region combination. It is safe to re-run — it is idempotent.

### 4.1 Run bootstrap

```bash
cd cdk
source .venv/bin/activate    # activate virtual environment if not already active

cdk bootstrap "aws://${AWS_ACCOUNT_ID}/${AWS_REGION}" \
  --region $AWS_REGION
```

What this creates in your account:
- **S3 bucket** named `cdk-hnb659fds-assets-<account>-<region>` — stores CloudFormation templates and Lambda zip packages
- **ECR repository** named `cdk-hnb659fds-container-assets-<account>-<region>` — stores the data generator Docker image
- **IAM roles** prefixed `cdk-hnb659fds-*` — used by CloudFormation to perform deployments
- **SSM parameter** `/cdk-bootstrap/hnb659fds/version` — tracks bootstrap version

Expected output:
```
 ⏳  Bootstrapping environment aws://123456789012/us-east-1...
Trusted accounts for deployment: (none)
Trusted accounts for lookup: (none)
Using default execution policy of 'arn:aws:iam::aws:policy/AdministratorAccess'
CDKToolkit: creating CloudFormation changeset...
 ✅  Environment aws://123456789012/us-east-1 bootstrapped.
```

### 4.2 Verify bootstrap succeeded

```bash
aws cloudformation describe-stacks \
  --stack-name CDKToolkit \
  --region $AWS_REGION \
  --query "Stacks[0].StackStatus" \
  --output text
# Expected: CREATE_COMPLETE  (or UPDATE_COMPLETE if previously bootstrapped)
```

```bash
cd ..    # return to repo root
```

---

## Step 5 — Deploy all CDK stacks

This is the main deployment step. CDK deploys all nine stacks in dependency order. The full deployment takes approximately **45–60 minutes**, dominated by:
- MSK cluster creation: ~15 minutes
- RDS PostgreSQL instance: ~8–12 minutes
- Neptune cluster: ~10 minutes
- OpenSearch domain: ~15 minutes
- SageMaker endpoints: ~5 minutes each

### 5.1 Understanding the deployment order

CDK resolves dependencies automatically and deploys in this order:

```
1. FraudDemo-Networking   ~3 min
2. FraudDemo-Storage      ~12 min  (RDS takes the longest)
3. FraudDemo-Streaming    ~17 min  (MSK cluster + topic Lambda)
4. FraudDemo-Graph        ~12 min  (Neptune, runs in parallel with Streaming)
5. FraudDemo-Search       ~17 min  (OpenSearch, runs in parallel with Streaming)
6. FraudDemo-ML           ~8 min   (SageMaker endpoints)
7. FraudDemo-Processing   ~5 min   (Flink apps — fast, just registers the config)
8. FraudDemo-Alerting     ~3 min   (Lambda, EventBridge, Step Functions)
9. FraudDemo-Compute      ~8 min   (ECS cluster + Docker image push)
```

### 5.2 Run the full deployment

```bash
cd cdk
source .venv/bin/activate

cdk deploy --all \
  --require-approval never \
  --region $AWS_REGION \
  --context region=$AWS_REGION \
  --context account=$AWS_ACCOUNT_ID \
  --context alertEmail=$ALERT_EMAIL \
  --context generatorScenario=mixed \
  --context generatorTps=200 \
  --context generatorAccounts=1000 \
  --context fraudInjectionRate=0.15
```

#### What each context parameter controls

| Parameter | Default | Description |
|-----------|---------|-------------|
| `region` | `us-east-1` | AWS region for all resources |
| `account` | (from CLI) | AWS account ID |
| `alertEmail` | `fraud-demo-alerts@example.com` | Email for SNS critical alert subscription |
| `generatorScenario` | `mixed` | Initial fraud scenario. Options: `mixed`, `normal`, `coordinated_ring`, `ato_attack`, `registration_burst`, `system_abuse` |
| `generatorTps` | `200` | Target events per second from the data generator |
| `generatorAccounts` | `1000` | Number of synthetic trading accounts to simulate |
| `fraudInjectionRate` | `0.15` | Fraction of accounts assigned to fraud personas (0.0–1.0) |
| `rdsInstanceClass` | `db.r5.large` | RDS instance type |
| `mskInstanceType` | `kafka.m5.large` | MSK broker instance type |
| `opensearchInstanceType` | `m5.large.search` | OpenSearch data node instance type |
| `sagemakerInstanceType` | `ml.m5.large` | SageMaker endpoint instance type |

### 5.3 Monitoring deployment progress

CDK prints progress to the terminal. You can also monitor in the AWS Console:

1. Open **CloudFormation** in the AWS Console
2. You will see stacks appearing one by one as CDK creates them
3. Click any stack → **Events** tab to see resource-level progress
4. Stacks with `CREATE_IN_PROGRESS` are still deploying; `CREATE_COMPLETE` means done

### 5.4 If the deployment fails mid-way

CDK will print the failing resource and the CloudFormation error. Common causes and fixes:

**`FraudDemo-ML` fails with `ValidationException: Unable to create model`**
→ The `model.tar.gz` files were not uploaded to S3 before deploying. Run Step 3 again, then:
```bash
cdk deploy FraudDemo-ML --require-approval never \
  --context region=$AWS_REGION --context account=$AWS_ACCOUNT_ID
```

**`FraudDemo-Streaming` fails with timeout on Kafka topic creation**
→ The MSK cluster was not ready when the topic-initializer Lambda ran. Re-deploy just this stack:
```bash
cdk deploy FraudDemo-Streaming --require-approval never \
  --context region=$AWS_REGION --context account=$AWS_ACCOUNT_ID
```

**`FraudDemo-Compute` fails with Docker build error**
→ Ensure Docker Desktop is running and you are authenticated to ECR:
```bash
aws ecr get-login-password --region $AWS_REGION | \
  docker login --username AWS --password-stdin \
  ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

cdk deploy FraudDemo-Compute --require-approval never \
  --context region=$AWS_REGION --context account=$AWS_ACCOUNT_ID
```

**Any stack rolls back with `ROLLBACK_COMPLETE`**
→ Delete the failed stack manually in the Console (Action → Delete), fix the underlying issue, then re-run `cdk deploy --all`.

```bash
cd ..    # return to repo root when done
```

---

## Step 6 — Verify stack outputs

Once all nine stacks show `CREATE_COMPLETE`, collect the key resource endpoints and identifiers. You will need these for the post-deployment configuration steps.

### 6.1 Retrieve all stack outputs in one command

```bash
aws cloudformation describe-stacks \
  --region $AWS_REGION \
  --query 'Stacks[?starts_with(StackName, `FraudDemo`)].{Stack:StackName, Outputs:Outputs[*].{Key:OutputKey, Value:OutputValue}}' \
  --output table
```

### 6.2 Save key outputs as environment variables

Run each of these individually and confirm the value is non-empty:

```bash
# VPC
export VPC_ID=$(aws cloudformation describe-stacks \
  --stack-name FraudDemo-Networking --region $AWS_REGION \
  --query "Stacks[0].Outputs[?OutputKey=='VpcId'].OutputValue" --output text)
echo "VPC: $VPC_ID"

# RDS endpoint
export RDS_ENDPOINT=$(aws cloudformation describe-stacks \
  --stack-name FraudDemo-Storage --region $AWS_REGION \
  --query "Stacks[0].Outputs[?OutputKey=='RdsEndpoint'].OutputValue" --output text)
echo "RDS: $RDS_ENDPOINT"

# MSK cluster ARN
export MSK_CLUSTER_ARN=$(aws cloudformation describe-stacks \
  --stack-name FraudDemo-Streaming --region $AWS_REGION \
  --query "Stacks[0].Outputs[?OutputKey=='MskClusterArn'].OutputValue" --output text)
echo "MSK ARN: $MSK_CLUSTER_ARN"

# MSK bootstrap servers
export MSK_BOOTSTRAP=$(aws kafka get-bootstrap-brokers \
  --cluster-arn $MSK_CLUSTER_ARN --region $AWS_REGION \
  --query "BootstrapBrokerStringSaslIam" --output text)
echo "MSK Bootstrap: $MSK_BOOTSTRAP"

# Neptune endpoint
export NEPTUNE_ENDPOINT=$(aws cloudformation describe-stacks \
  --stack-name FraudDemo-Graph --region $AWS_REGION \
  --query "Stacks[0].Outputs[?OutputKey=='NeptuneEndpoint'].OutputValue" --output text)
echo "Neptune: $NEPTUNE_ENDPOINT"

# OpenSearch endpoint
export OPENSEARCH_ENDPOINT=$(aws cloudformation describe-stacks \
  --stack-name FraudDemo-Search --region $AWS_REGION \
  --query "Stacks[0].Outputs[?OutputKey=='OpenSearchEndpoint'].OutputValue" --output text)
echo "OpenSearch: $OPENSEARCH_ENDPOINT"
echo "Dashboard URL: https://$OPENSEARCH_ENDPOINT/_dashboards"

# ECS cluster / service
export ECS_CLUSTER=$(aws cloudformation describe-stacks \
  --stack-name FraudDemo-Compute --region $AWS_REGION \
  --query "Stacks[0].Outputs[?OutputKey=='EcsClusterName'].OutputValue" --output text)
echo "ECS Cluster: $ECS_CLUSTER"
```

### 6.3 Confirm MSK Kafka topics were created

The `FraudDemo-Streaming` stack deploys a Lambda function (`fraud-demo-topic-initializer`) as a CloudFormation custom resource that runs immediately after the MSK cluster is ready and creates the seven required topics. Verify they exist:

```bash
aws kafka list-clusters \
  --cluster-name-filter "fraud-demo-cluster" \
  --region $AWS_REGION \
  --query "ClusterInfoList[0].ClusterArn" \
  --output text
```

To inspect the topic-initializer Lambda execution logs:
```bash
aws logs filter-log-events \
  --log-group-name "/aws/lambda/fraud-demo-topic-initializer" \
  --region $AWS_REGION \
  --query "events[*].message" \
  --output text
```

You should see lines like:
```
Created topics: dict_keys(['trades.raw', 'sessions.raw', 'registrations.raw',
  'api.calls', 'alerts.fraud', 'features.realtime', 'dlq.processing-errors'])
```

If the log shows `TopicAlreadyExistsError`, the topics already exist from a previous deployment — that is fine.

### 6.4 Verify SageMaker endpoints are InService

All three SageMaker endpoints must be in `InService` status before the Flink apps can call them. Check:

```bash
for ep in fraud-demo-coordinated-trading fraud-demo-login-risk fraud-demo-registration-anomaly; do
  STATUS=$(aws sagemaker describe-endpoint \
    --endpoint-name $ep \
    --region $AWS_REGION \
    --query "EndpointStatus" --output text)
  echo "$ep: $STATUS"
done
```

Expected:
```
fraud-demo-coordinated-trading: InService
fraud-demo-login-risk: InService
fraud-demo-registration-anomaly: InService
```

If any show `Creating`, wait 3–5 minutes and check again. If any show `Failed`, check the SageMaker console for error details — the most common cause is missing `model.tar.gz` in S3 (see Step 3.4).

---

## Step 7 — Confirm SNS email subscription

The `FraudDemo-Alerting` stack creates an SNS topic (`fraud-demo-critical-alerts`) and subscribes your `$ALERT_EMAIL` address to it. AWS sends a confirmation email immediately after the stack deploys. **You must click the confirmation link** for email notifications to work during the demo.

### 7.1 Find the confirmation email

- Check your inbox at `$ALERT_EMAIL` for a message from `AWS Notifications <no-reply@sns.amazonaws.com>`
- Subject: `AWS Notification - Subscription Confirmation`
- **Click the "Confirm subscription" link** in the email body

The link is valid for 3 days. If it has expired or you did not receive it, re-send the confirmation:

```bash
# Get the subscription ARN
SUBSCRIPTION_ARN=$(aws sns list-subscriptions-by-topic \
  --topic-arn "arn:aws:sns:${AWS_REGION}:${AWS_ACCOUNT_ID}:fraud-demo-critical-alerts" \
  --region $AWS_REGION \
  --query "Subscriptions[0].SubscriptionArn" --output text)

echo "Subscription ARN: $SUBSCRIPTION_ARN"
# If this shows 'PendingConfirmation', the email has not been confirmed yet
```

### 7.2 Send a test notification

After confirming the subscription, send a test message to verify end-to-end email delivery:

```bash
aws sns publish \
  --topic-arn "arn:aws:sns:${AWS_REGION}:${AWS_ACCOUNT_ID}:fraud-demo-critical-alerts" \
  --subject "[TEST] Fraud Detection Demo — Test Alert" \
  --message "This is a test notification from the Fraud Detection Demo deployment. If you received this, email alerting is working correctly." \
  --region $AWS_REGION
```

Check your inbox for the test email within 30 seconds.

---

## Step 8 — Set up OpenSearch indices and dashboards

The OpenSearch domain is deployed and running, but it needs index templates to be created before it can accept fraud event and alert documents. The `setup_opensearch.py` script creates the required index templates with correct field mappings.

### 8.1 Retrieve the OpenSearch admin password

The admin password was auto-generated by Secrets Manager during the `FraudDemo-Search` stack deployment. Retrieve it:

```bash
export OPENSEARCH_PASSWORD=$(aws secretsmanager get-secret-value \
  --secret-id "fraud-demo/opensearch/admin" \
  --region $AWS_REGION \
  --query "SecretString" --output text | python3 -c "import sys,json; print(json.load(sys.stdin)['password'])")

echo "OpenSearch admin password: $OPENSEARCH_PASSWORD"
# Store this somewhere safe for the demo
```

### 8.2 Run the setup script

```bash
python scripts/setup_opensearch.py \
  --endpoint $OPENSEARCH_ENDPOINT \
  --region $AWS_REGION
```

The script performs the following:
1. Checks cluster health — waits until status is `yellow` or `green`
2. Creates the `fraud-events` index template (covers `fraud-events-YYYY.MM.DD` daily indices)
3. Creates the `fraud-alerts` index template (covers `fraud-alerts-YYYY.MM.DD` daily indices)

Expected output:
```
[INFO] Configuring OpenSearch at: search-fraud-demo-search-xxxxx.us-east-1.es.amazonaws.com
[INFO] Cluster health: yellow
[INFO] Creating index template: fraud-events
[INFO] Creating index template: fraud-alerts
[INFO] OpenSearch setup complete.
```

> A `yellow` cluster health status is normal for a single-AZ OpenSearch domain with replicas — it means primary shards are allocated but replica shards have nowhere to go (no second node in the same AZ). This does not affect functionality.



### 8.3 Log in to OpenSearch Dashboards

The OpenSearch domain lives inside the VPC and has no public endpoint. You need a network tunnel from your laptop to reach the Dashboards UI. Two options are available:

**Option A - SSH tunnel via a bastion host (if one exists in the VPC)**

```bash
# Replace placeholders with real values
ssh -L 5601:<OPENSEARCH_ENDPOINT>:443 ec2-user@<BASTION_PUBLIC_IP> -N &
```

Then open in your browser: `https://localhost:5601/_dashboards`

**Option B - AWS Systems Manager Session Manager port-forwarding (no bastion needed)**

This works as long as any EC2 instance in the VPC has the SSM agent installed (e.g. the ECS worker nodes).

```bash
# Find an instance ID in the private subnet
INSTANCE_ID=$(aws ec2 describe-instances \
  --filters "Name=vpc-id,Values=$VPC_ID" "Name=instance-state-name,Values=running" \
  --region $AWS_REGION \
  --query "Reservations[0].Instances[0].InstanceId" --output text)

# Start port-forwarding session
aws ssm start-session \
  --target $INSTANCE_ID \
  --document-name AWS-StartPortForwardingSessionToRemoteHost \
  --parameters "host=$OPENSEARCH_ENDPOINT,portNumber=443,localPortNumber=5601" \
  --region $AWS_REGION
```

Then open: `https://localhost:5601/_dashboards`

**Login credentials:**
- Username: `fraud_admin`
- Password: the value of `$OPENSEARCH_PASSWORD` from Step 8.1

On first login, when asked to select a tenant choose **Global**.

### 8.4 Create index patterns in OpenSearch Dashboards

Before any documents are visible in Discover or Visualize, you must tell Dashboards which indices to query by creating index patterns.

1. Click the **hamburger menu** (three lines, top-left corner)
2. Scroll down to **Stack Management** and click it
3. Under the **Kibana** heading, click **Index Patterns**
4. Click the blue **Create index pattern** button
5. In the *Index pattern name* field enter: `fraud-alerts-*`
6. Click **Next step**
7. In the *Time field* dropdown select: `timestamp`
8. Click **Create index pattern**
9. Repeat steps 4-8 for: `fraud-events-*`

To verify the index templates are correctly registered via CLI:

```bash
curl -u "fraud_admin:${OPENSEARCH_PASSWORD}" \
  -k "https://${OPENSEARCH_ENDPOINT}/_index_template?pretty" \
  | python3 -c "import sys,json; [print(t['name']) for t in json.load(sys.stdin)['index_templates'] if 'fraud' in t['name']]"
```

Expected output:
```
fraud-events
fraud-alerts
```

---

## Step 9 - Start the Flink detection applications

The three Managed Flink applications are registered by the `FraudDemo-Processing` stack in `READY` status. They do not start automatically and must be started explicitly. Each application takes approximately 2-4 minutes to transition from `STARTING` to `RUNNING`.

### 9.1 What the three applications do

| Application name | Reads from Kafka | Detects | Calls SageMaker endpoint |
|---|---|---|---|
| `fraud-demo-coordinated-trading` | `trades.raw` | Coordinated ring activity, spread/latency abuse, correlated P&L across accounts | `fraud-demo-coordinated-trading` |
| `fraud-demo-system-abuse` | `registrations.raw`, `api.calls` | API rate hammering, order/cancel cycling, registration bursts, shared device fingerprints, bonus farming | `fraud-demo-registration-anomaly` |
| `fraud-demo-account-takeover` | `sessions.raw` | Impossible geo-velocity, new device + withdrawal chain, credential stuffing burst, password reset + transfer CEP | `fraud-demo-login-risk` |

All three applications write fraud alert messages to the `alerts.fraud` Kafka topic. That topic is consumed by the Lambda alert processor (`fraud-demo-alert-processor`), which writes to DynamoDB, indexes into OpenSearch, and fans out to SNS.

### 9.2 Inject the live MSK bootstrap servers into each application

The applications were deployed with a configuration placeholder `${MSK_BOOTSTRAP_SERVERS}` because the MSK broker addresses are only known after the cluster is created. You must update each application with the real IAM-auth bootstrap broker string before starting.

Make sure `$MSK_BOOTSTRAP` is set from Step 6.2, then run:

```bash
for APP in fraud-demo-coordinated-trading fraud-demo-system-abuse fraud-demo-account-takeover; do
  echo "Fetching current version for $APP..."

  CURRENT_VERSION=$(aws kinesisanalyticsv2 describe-application \
    --application-name $APP \
    --region $AWS_REGION \
    --query "ApplicationDetail.ApplicationVersionId" \
    --output text)

  echo "  Current version: $CURRENT_VERSION"
  echo "  Injecting bootstrap servers: $MSK_BOOTSTRAP"

  aws kinesisanalyticsv2 update-application \
    --application-name $APP \
    --region $AWS_REGION \
    --current-application-version-id $CURRENT_VERSION \
    --application-configuration-update \
      '{"EnvironmentPropertyUpdates": {"PropertyGroupUpdates": [
         {"PropertyGroupId": "KafkaSource", "PropertyMap": {"bootstrap.servers": "'"$MSK_BOOTSTRAP"'"}},
         {"PropertyGroupId": "KafkaSink",   "PropertyMap": {"bootstrap.servers": "'"$MSK_BOOTSTRAP"'"}}
      ]}}'

  echo "  Done - $APP updated."
  echo ""
done
```

Confirm each update succeeded by checking the new version number incremented:

```bash
for APP in fraud-demo-coordinated-trading fraud-demo-system-abuse fraud-demo-account-takeover; do
  VERSION=$(aws kinesisanalyticsv2 describe-application \
    --application-name $APP --region $AWS_REGION \
    --query "ApplicationDetail.ApplicationVersionId" --output text)
  echo "$APP version: $VERSION"
done
```

### 9.3 Start all three Flink applications

```bash
for APP in fraud-demo-coordinated-trading fraud-demo-system-abuse fraud-demo-account-takeover; do
  echo "Starting $APP..."
  aws kinesisanalyticsv2 start-application \
    --application-name $APP \
    --run-configuration '{}' \
    --region $AWS_REGION
  echo "  Start command issued for $APP"
done
```

The CLI returns immediately after issuing the start command. The application will be in `STARTING` status for the next 2-4 minutes.

### 9.4 Poll until all applications reach RUNNING

```bash
ALL_RUNNING=false
ATTEMPTS=0
while [ "$ALL_RUNNING" != "true" ] && [ $ATTEMPTS -lt 30 ]; do
  ALL_RUNNING=true
  ATTEMPTS=$((ATTEMPTS+1))
  for APP in fraud-demo-coordinated-trading fraud-demo-system-abuse fraud-demo-account-takeover; do
    STATUS=$(aws kinesisanalyticsv2 describe-application \
      --application-name $APP --region $AWS_REGION \
      --query "ApplicationDetail.ApplicationStatus" --output text)
    echo "$APP: $STATUS"
    if [ "$STATUS" != "RUNNING" ]; then ALL_RUNNING=false; fi
  done
  if [ "$ALL_RUNNING" != "true" ]; then
    echo "Not all RUNNING yet, waiting 15 seconds..."
    sleep 15
    echo "---"
  fi
done

if [ "$ALL_RUNNING" = "true" ]; then
  echo "All Flink applications are RUNNING."
else
  echo "WARNING: Some applications did not reach RUNNING status after 7.5 minutes."
fi
```

### 9.5 Verify Flink logs show a healthy start

Check for any ERROR-level log entries that appeared in the last 5 minutes:

```bash
START_TIME=$(( $(date +%s) * 1000 - 300000 ))

for APP in fraud-demo-coordinated-trading fraud-demo-system-abuse fraud-demo-account-takeover; do
  echo "=== $APP errors ==="
  ERRORS=$(aws logs filter-log-events \
    --log-group-name "/aws/flink/$APP" \
    --start-time $START_TIME \
    --filter-pattern "ERROR" \
    --region $AWS_REGION \
    --query "events[*].message" \
    --output text 2>/dev/null)
  if [ -z "$ERRORS" ]; then
    echo "  No errors found - healthy"
  else
    echo "$ERRORS" | head -5
  fi
done
```

Expected: `No errors found - healthy` for all three.

Key healthy startup lines to look for (use `--filter-pattern "RUNNING"` instead of `"ERROR"`):
```
Source: MSK-trades switched from CREATED to RUNNING
Sink: fraud-alerts switched from CREATED to RUNNING
Job has been submitted with JobID <id>
```

If you see `ClassNotFoundException`, the Flink JAR artifact is missing from the S3 `flink-artifacts` bucket. The JAR must be placed at:
- `s3://fraud-demo-flink-<account>-<region>/flink-apps/coordinated-trading-1.0.0.jar`
- `s3://fraud-demo-flink-<account>-<region>/flink-apps/system-abuse-1.0.0.jar`
- `s3://fraud-demo-flink-<account>-<region>/flink-apps/account-takeover-1.0.0.jar`

---

## Step 10 - Verify the data generator is running

The synthetic data generator runs as an ECS Fargate service (`fraud-demo-datagen`) which is started automatically during the `FraudDemo-Compute` stack deployment. On its very first launch the container:

1. Uploads GeoIP and IP reputation reference data to the S3 enrichment bucket
2. Seeds the RDS `fraud_detection` database with 1,000 synthetic trading accounts
3. Generates 30 days of login baseline history for every account (used by the ATO model)
4. Begins the continuous event generation loop, emitting to Kafka at the configured TPS

### 10.1 Confirm the ECS service has a running task

```bash
aws ecs describe-services \
  --cluster fraud-demo-datagen \
  --services fraud-demo-datagen \
  --region $AWS_REGION \
  --query "services[0].{Status:status, Running:runningCount, Desired:desiredCount, LastDeployment:deployments[0].rolloutState}" \
  --output table
```

Expected output:
```
---------------------------------------------------
|            DescribeServices                     |
+------------------+--------+---------+-----------+
| LastDeployment   | Desired | Running | Status   |
+------------------+--------+---------+-----------+
| COMPLETED        | 1       | 1       | ACTIVE   |
+------------------+--------+---------+-----------+
```

If `Running = 0` the task has stopped. This almost always means a startup error. Check logs immediately (Step 10.2).

### 10.2 Tail the generator container logs

```bash
aws logs tail "/ecs/fraud-demo-datagen" \
  --follow \
  --format short \
  --region $AWS_REGION
```

Press `Ctrl+C` to stop tailing after you have confirmed the generator is healthy.

**A healthy startup sequence looks like this:**
```
datagen [INFO] Config: scenario=mixed, tps=200, accounts=1000, fraud_rate=0.15
datagen [INFO] Uploading enrichment data to s3://fraud-demo-enrichment-.../
datagen [INFO] S3 enrichment data upload complete.
datagen [INFO] Seeding RDS with 1000 accounts...
datagen [INFO] RDS seeding complete.
datagen [INFO] Personas built: NormalTrader=750, CoordinatedRingMember=80,
                               ATOAttacker=70, AbusiveRegistrant=60, SystemAbuser=40
datagen [INFO] Starting event loop - target 200 TPS across 1000 personas
datagen [INFO] Sent 2000 events | actual TPS: 197.3
datagen [INFO] Sent 4000 events | actual TPS: 199.1
datagen [INFO] Sent 6000 events | actual TPS: 200.8
```

The `Sent N events | actual TPS` line repeats every 10 seconds indefinitely. Once you see it, the generator is working correctly.

**Seeding typically takes 2-4 minutes** on first run. If you only see the `Config` line and nothing after it for more than 5 minutes, the container has likely hung on an RDS or S3 connection. Check the error in the logs and refer to the troubleshooting table below.

### 10.3 Diagnosing common generator startup failures

| Error message in logs | Root cause | Resolution |
|---|---|---|
| `could not connect to server: Connection refused (port 5432)` | RDS is in isolated subnets and the ECS security group is not permitted | Go to EC2 console, find security group `fraud-demo-rds-sg`, confirm it has an inbound rule for TCP port 5432 from source `fraud-demo-ecs-sg` |
| `NoBrokersAvailable` | MSK broker is not reachable from the ECS task | Confirm `fraud-demo-msk-sg` has an inbound rule for TCP ports 9092-9098 from source `fraud-demo-ecs-sg` |
| `AccessDeniedException: User is not authorized to perform secretsmanager:GetSecretValue` | The ECS task role is missing the Secrets Manager permission | In IAM, find role `fraud-demo-datagen-task` and confirm it has an inline policy allowing `secretsmanager:GetSecretValue` on the RDS credentials secret ARN |
| `SSL SYSCALL error: EOF detected` | PostgreSQL SSL handshake failed | Confirm RDS instance parameter group has `rds.force_ssl=1` (the CDK stack sets this by default) |
| `KafkaTimeoutError: Failed to update metadata after 60.0 secs` | The MSK cluster ARN environment variable points to the wrong cluster or the cluster is still starting | Verify the `MSK_CLUSTER_ARN` environment variable in the ECS task definition matches the ARN in the `FraudDemo-Streaming` stack output |

### 10.4 Confirm events are being consumed by Flink

The most direct confirmation that the full pipeline (generator -> Kafka -> Flink) is working is to check the Flink application's record consumption metric in CloudWatch.

**In the AWS Console:**
1. Open **CloudWatch**
2. Click **Metrics** -> **All metrics**
3. Find namespace **AWS/KinesisAnalytics**
4. Click **Application Metrics**
5. Find `ApplicationName = fraud-demo-account-takeover`, metric `numRecordsInPerSecond`
6. Click the checkbox to add it to the graph

This metric should show a steady positive value (approximately your configured TPS divided by the number of Flink apps consuming that topic). Allow 1-2 minutes from generator startup for the first data points to appear.

**Via CLI (requires CloudWatch metric query):**
```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/KinesisAnalytics \
  --metric-name numRecordsInPerSecond \
  --dimensions Name=Application,Value=fraud-demo-account-takeover \
  --start-time $(date -u -v-5M +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u --date='5 minutes ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 60 \
  --statistics Average \
  --region $AWS_REGION \
  --query "Datapoints[*].Average" \
  --output text
```

Expected: one or more values greater than 0.

---

## Step 11 - End-to-end smoke test

This step verifies the entire alert pipeline is wired correctly before the demo. You inject one synthetic fraud alert directly through EventBridge (bypassing Kafka and Flink) and verify it flows correctly through Lambda to DynamoDB, OpenSearch, SNS, and Step Functions.

### 11.1 Send a test CRITICAL alert via EventBridge

```bash
ALERT_ID="smoke-test-$(date +%s)"

aws events put-events \
  --region $AWS_REGION \
  --entries "[
    {
      \"Source\": \"fraud.detection\",
      \"DetailType\": \"FraudAlert\",
      \"EventBusName\": \"fraud-detection-demo\",
      \"Detail\": \"{
        \\\"alert_id\\\": \\\"$ALERT_ID\\\",
        \\\"entity_id\\\": \\\"test-account-smoke\\\",
        \\\"entity_type\\\": \\\"ACCOUNT\\\",
        \\\"typology\\\": \\\"ACCOUNT_TAKEOVER\\\",
        \\\"severity\\\": \\\"CRITICAL\\\",
        \\\"risk_score\\\": 0.97,
        \\\"signals\\\": [\\\"GEO_VELOCITY_IMPOSSIBLE\\\", \\\"NEW_DEVICE\\\"],
        \\\"detection_method\\\": \\\"RULE\\\",
        \\\"timestamp\\\": \\\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\\\"
      }\"
    }
  ]"

echo "Injected alert ID: $ALERT_ID"
```

### 11.2 Verify the Lambda processed it (CloudWatch Logs)

```bash
sleep 10  # wait for Lambda to execute

aws logs filter-log-events \
  --log-group-name "/aws/lambda/fraud-demo-alert-processor" \
  --start-time $(( $(date +%s) * 1000 - 60000 )) \
  --region $AWS_REGION \
  --filter-pattern "smoke-test" \
  --query "events[*].message" \
  --output text
```

Expected: A line containing `Alert smoke-test-<timestamp> processed: CRITICAL | ACCOUNT_TAKEOVER`

### 11.3 Verify the alert was written to DynamoDB

```bash
aws dynamodb query \
  --table-name fraud-demo-alerts \
  --index-name by-entity \
  --region $AWS_REGION \
  --key-condition-expression "entity_id = :e" \
  --expression-attribute-values '{":e": {"S": "test-account-smoke"}}' \
  --query "Items[*].{AlertId:alert_id.S, Severity:severity.S, Typology:typology.S, Status:status.S}" \
  --output table
```

Expected:
```
--------------------------------------------------------------------
|  AlertId             | Severity | Typology          | Status   |
+----------------------+----------+-------------------+----------+
| smoke-test-<ts>      | CRITICAL | ACCOUNT_TAKEOVER  | NEW      |
--------------------------------------------------------------------
```

### 11.4 Verify the alert was indexed in OpenSearch

```bash
curl -u "fraud_admin:${OPENSEARCH_PASSWORD}" -s \
  -k "https://${OPENSEARCH_ENDPOINT}/fraud-alerts-*/_search?q=entity_id:test-account-smoke&pretty" \
  | python3 -c "import sys,json; r=json.load(sys.stdin); print('Hits:', r['hits']['total']['value'])"
```

Expected: `Hits: 1`

### 11.5 Verify the CRITICAL alert triggered an SNS email

Because the test event has `severity: CRITICAL`, the EventBridge rule `fraud-demo-critical-to-sns` should have forwarded it to SNS, which sends an email.

Check your `$ALERT_EMAIL` inbox for a subject line beginning with `[CRITICAL] Fraud Alert`. This typically arrives within 30-60 seconds of the event being injected.

### 11.6 Verify the Step Functions investigation workflow executed

```bash
aws stepfunctions list-executions \
  --state-machine-arn "arn:aws:states:${AWS_REGION}:${AWS_ACCOUNT_ID}:stateMachine:fraud-demo-investigation" \
  --max-results 1 \
  --region $AWS_REGION \
  --query "executions[0].{Name:name, Status:status, Start:startDate}" \
  --output table
```

Expected: `Status = SUCCEEDED`

If you see `FAILED`, inspect the execution:
```bash
EXEC_ARN=$(aws stepfunctions list-executions \
  --state-machine-arn "arn:aws:states:${AWS_REGION}:${AWS_ACCOUNT_ID}:stateMachine:fraud-demo-investigation" \
  --max-results 1 --region $AWS_REGION \
  --query "executions[0].executionArn" --output text)

aws stepfunctions get-execution-history \
  --execution-arn $EXEC_ARN \
  --region $AWS_REGION \
  --query "events[?type=='ExecutionFailed'].executionFailedEventDetails" \
  --output json
```

### 11.7 Deployment smoke test checklist

All of the following should be green before proceeding to a demo:

| # | Check | How to verify | Expected |
|---|---|---|---|
| 1 | ECS data generator running | `aws ecs describe-services` | `runningCount: 1` |
| 2 | Generator producing events | CloudWatch Logs `/ecs/fraud-demo-datagen` | TPS lines every 10s |
| 3 | All Flink apps RUNNING | `aws kinesisanalyticsv2 describe-application` x3 | `RUNNING` |
| 4 | Flink consuming from Kafka | CloudWatch metric `numRecordsInPerSecond` | > 0 |
| 5 | All SageMaker endpoints InService | `aws sagemaker describe-endpoint` x3 | `InService` |
| 6 | Lambda alert processor working | CloudWatch Logs `/aws/lambda/fraud-demo-alert-processor` | No errors |
| 7 | DynamoDB alert write | Query `fraud-demo-alerts` by entity | Record present |
| 8 | OpenSearch indexed | `curl` search | 1 hit |
| 9 | SNS email received | Email inbox | Email within 60s |
| 10 | Step Functions succeeded | Console or CLI | `SUCCEEDED` |

---


## Stack-by-stack resource reference

Use this table to quickly locate any resource in the AWS Console or CLI.

### FraudDemo-Networking

| Resource | Name / Identifier | Detail |
|---|---|---|
| VPC | `fraud-demo-vpc` | CIDR `10.0.0.0/16`, 3 AZs |
| Public subnets | `fraud-demo-vpc/PublicSubnet1/2/3` | `/24` each, `MapPublicIpOnLaunch=true` |
| Private subnets | `fraud-demo-vpc/PrivateSubnet1/2/3` | `/22` each, NAT gateway egress |
| Isolated subnets | `fraud-demo-vpc/IsolatedSubnet1/2/3` | `/24` each, no internet access |
| NAT Gateways | 2 gateways | One per AZ (AZ1 and AZ2), each with an EIP |
| Security group | `fraud-demo-msk-sg` | Allows TCP 9092-9098 inbound from Flink, Lambda, ECS SGs |
| Security group | `fraud-demo-flink-sg` | Flink application network identity |
| Security group | `fraud-demo-rds-sg` | Allows TCP 5432 inbound from Flink, Lambda, ECS SGs |
| Security group | `fraud-demo-neptune-sg` | Allows TCP 8182 inbound from Flink, Lambda SGs |
| Security group | `fraud-demo-opensearch-sg` | Allows TCP 443 inbound from Flink, Lambda SGs |
| Security group | `fraud-demo-lambda-sg` | Lambda function network identity |
| Security group | `fraud-demo-ecs-sg` | ECS Fargate task network identity |
| VPC Endpoint | S3 Gateway | Free gateway endpoint; routes S3 traffic off NAT |
| VPC Endpoint | DynamoDB Gateway | Free gateway endpoint; routes DDB traffic off NAT |
| VPC Endpoint | SageMaker Runtime Interface | Private DNS enabled; Flink calls SageMaker in-VPC |
| VPC Endpoint | Secrets Manager Interface | Private DNS enabled |
| VPC Endpoint | CloudWatch Logs Interface | Private DNS enabled |

### FraudDemo-Storage

| Resource | Name | Detail |
|---|---|---|
| RDS instance | `fraud-demo-db` | PostgreSQL 15.4, `db.r5.large`, isolated subnets, logical replication enabled for CDC |
| Secrets Manager | `fraud-demo/rds/credentials` | JSON with `username`, `password`, `host`, `port`, `dbname` |
| S3 bucket | `fraud-demo-raw-<account>-<region>` | Raw event archive, 30-day expiry lifecycle rule |
| S3 bucket | `fraud-demo-ml-<account>-<region>` | ML model artifacts, versioning enabled |
| S3 bucket | `fraud-demo-flink-<account>-<region>` | Flink JAR artifacts, versioning enabled |
| S3 bucket | `fraud-demo-enrichment-<account>-<region>` | GeoIP, IP reputation, email blocklist reference data |
| DynamoDB table | `fraud-demo-entity-state` | PK: `entity_id` (S), SK: `entity_type` (S), TTL: `ttl`, PAY_PER_REQUEST |
| DynamoDB table | `fraud-demo-sessions` | PK: `session_id` (S), TTL: `ttl`, PAY_PER_REQUEST |
| DynamoDB table | `fraud-demo-velocity` | PK: `counter_key` (S), SK: `window_start` (N), TTL: `ttl`, PAY_PER_REQUEST |
| DynamoDB table | `fraud-demo-alerts` | PK: `alert_id` (S), SK: `created_at` (N), GSIs: `by-entity`, `by-typology`, `by-severity` |

### FraudDemo-Streaming

| Resource | Name | Detail |
|---|---|---|
| MSK cluster | `fraud-demo-cluster` | 3 brokers, `kafka.m5.large`, Kafka 3.6.0, IAM auth only, TLS in-transit |
| MSK config | `fraud-demo-msk-config` | 24h log retention, lz4 compression, auto.create.topics=false |
| Kafka topic | `trades.raw` | 12 partitions, RF 2 ? raw trade events |
| Kafka topic | `sessions.raw` | 6 partitions, RF 2 ? login/logout/session events |
| Kafka topic | `registrations.raw` | 3 partitions, RF 2 ? account registration events |
| Kafka topic | `api.calls` | 12 partitions, RF 2 ? API call events |
| Kafka topic | `alerts.fraud` | 6 partitions, RF 2 ? fraud alert output |
| Kafka topic | `features.realtime` | 6 partitions, RF 2 ? real-time feature vectors |
| Kafka topic | `dlq.processing-errors` | 3 partitions, RF 2 ? dead letter queue |
| Lambda function | `fraud-demo-topic-initializer` | Python 3.11, VPC-attached, runs once at deploy via CloudFormation custom resource |
| CloudWatch log group | `/aws/msk/fraud-demo` | 7-day retention |

### FraudDemo-Graph

| Resource | Name | Detail |
|---|---|---|
| Neptune cluster | `fraud-demo-graph` | 1 instance `db.r5.large`, isolated subnets, IAM auth, deletion protection off |
| IAM role | `fraud-demo-neptune-loader` | Grants Neptune permission to bulk-load from S3 |

### FraudDemo-Search

| Resource | Name | Detail |
|---|---|---|
| OpenSearch domain | `fraud-demo-search` | 2 x `m5.large.search`, 100 GB GP3 EBS each, OpenSearch 2.11, node-to-node encryption, encryption at rest |
| Secrets Manager | `fraud-demo/opensearch/admin` | Auto-generated 16-char alphanumeric password |

### FraudDemo-ML

| Resource | Name | Detail |
|---|---|---|
| IAM role | `fraud-demo-sagemaker-execution` | `AmazonSageMakerFullAccess` + read/write on ML artifacts bucket |
| Feature group | `fraud-demo-account-trading-profile` | 13 features, online store enabled, offline to S3 |
| Feature group | `fraud-demo-login-behavior` | 10 features, online store enabled |
| Feature group | `fraud-demo-registration-profile` | 10 features, online store enabled |
| SageMaker model | `fraud-demo-coordinated-trading-model` | sklearn 1.2 container, Isolation Forest, artifact at `models/coordinated_trading/model.tar.gz` |
| SageMaker model | `fraud-demo-login-risk-model` | sklearn 1.2 container, Random Forest, artifact at `models/login_risk/model.tar.gz` |
| SageMaker model | `fraud-demo-registration-anomaly-model` | sklearn 1.2 container, Autoencoder, artifact at `models/registration_anomaly/model.tar.gz` |
| SageMaker endpoint | `fraud-demo-coordinated-trading` | 1 x `ml.m5.large`, real-time inference |
| SageMaker endpoint | `fraud-demo-login-risk` | 1 x `ml.m5.large`, real-time inference |
| SageMaker endpoint | `fraud-demo-registration-anomaly` | 1 x `ml.m5.large`, real-time inference |

### FraudDemo-Processing

| Resource | Name | Detail |
|---|---|---|
| IAM role | `fraud-demo-flink-execution` | MSK read/write, SageMaker invoke, DynamoDB read/write, Neptune access, S3 read, CloudWatch logs |
| Flink application | `fraud-demo-coordinated-trading` | Flink 1.18, parallelism 4, checkpointing every 60s, reads `trades.raw` |
| Flink application | `fraud-demo-system-abuse` | Flink 1.18, parallelism 2, checkpointing every 60s, reads `registrations.raw` + `api.calls` |
| Flink application | `fraud-demo-account-takeover` | Flink 1.18, parallelism 2, checkpointing every 60s, reads `sessions.raw` |
| CloudWatch log group | `/aws/flink/fraud-demo-coordinated-trading` | 7-day retention |
| CloudWatch log group | `/aws/flink/fraud-demo-system-abuse` | 7-day retention |
| CloudWatch log group | `/aws/flink/fraud-demo-account-takeover` | 7-day retention |

### FraudDemo-Alerting

| Resource | Name | Detail |
|---|---|---|
| EventBridge bus | `fraud-detection-demo` | Custom event bus, source `fraud.detection`, detail-type `FraudAlert` |
| SNS topic | `fraud-demo-critical-alerts` | Email subscription to `alertEmail` context parameter |
| SNS topic | `fraud-demo-standard-alerts` | No default subscription |
| Lambda function | `fraud-demo-alert-processor` | Python 3.11, VPC-attached, writes to DynamoDB + OpenSearch, publishes to SNS |
| Step Functions | `fraud-demo-investigation` | CRITICAL -> AutoBlock -> RecordDecision; HIGH -> FlagForReview; else MonitorWatchlist |
| EventBridge rule | `fraud-demo-critical-to-sns` | CRITICAL severity alerts -> SNS critical topic |
| EventBridge rule | `fraud-demo-all-alerts-processor` | All alerts -> Lambda processor |
| EventBridge rule | `fraud-demo-all-alerts-investigation` | All alerts -> Step Functions |
| CloudWatch log group | `/aws/states/fraud-demo-investigation` | 7-day retention |

### FraudDemo-Compute

| Resource | Name | Detail |
|---|---|---|
| ECS cluster | `fraud-demo-datagen` | Container Insights enabled, Fargate launch type |
| ECS service | `fraud-demo-datagen` | 1 desired task, private subnet, no public IP |
| Fargate task definition | `fraud-demo-datagen` | 1 vCPU, 2 GB memory, 1 container |
| IAM task role | `fraud-demo-datagen-task` | MSK write, S3 write to raw bucket, S3 read from enrichment bucket, Secrets Manager read |
| CloudWatch log group | `/ecs/fraud-demo-datagen` | 7-day retention |

---

## CDK context parameters reference

All parameters below can be overridden at deploy time with `--context key=value`. They can also be made permanent by adding them to the `context` section of `cdk/cdk.json`.

| Parameter | Type | Default value | Description |
|---|---|---|---|
| `region` | string | `us-east-1` | AWS region for all resources. Must be a region where all required services (MSK, Neptune, Managed Flink, SageMaker, OpenSearch) are available. |
| `account` | string | resolved from CLI | AWS account ID. Set explicitly to avoid ambiguity when multiple CLI profiles are configured. |
| `alertEmail` | string | `fraud-demo-alerts@example.com` | The email address subscribed to the `fraud-demo-critical-alerts` SNS topic. You must confirm the subscription before the demo. |
| `generatorScenario` | string | `mixed` | Starting scenario for the ECS data generator. Options: `mixed` (all personas), `normal` (no fraud), `coordinated_ring`, `ato_attack`, `registration_burst`, `system_abuse`. |
| `generatorTps` | integer | `200` | Target events per second emitted by the data generator. Reduce to `50` for low-cost demos; increase to `500+` for high-volume stress testing. |
| `generatorAccounts` | integer | `1000` | Total number of synthetic trading accounts to simulate. More accounts means more realistic population distribution but higher RDS seed time on first startup. |
| `fraudInjectionRate` | float | `0.15` | Fraction of accounts (0.0-1.0) assigned to fraud personas. `0.15` means 15% fraud, 85% normal. |
| `rdsInstanceClass` | string | `db.r5.large` | RDS PostgreSQL instance type. Use `db.t4g.medium` for lower cost if trade volume is low. |
| `mskInstanceType` | string | `kafka.m5.large` | MSK broker instance type. Use `kafka.t3.small` for lowest cost (limited to 1 broker though). |
| `opensearchInstanceType` | string | `m5.large.search` | OpenSearch data node instance type. `m5.large.search` is the minimum for reliable performance. |
| `sagemakerInstanceType` | string | `ml.m5.large` | SageMaker real-time endpoint instance type. `ml.m5.large` provides adequate inference throughput for demo volumes. |
| `environment` | string | `demo` | Value of the `Environment` tag applied to all resources. Useful for cost allocation. |

**Example ? deploying a lower-cost configuration:**

```bash
cdk deploy --all --require-approval never \
  --context region=us-east-1 \
  --context alertEmail=you@example.com \
  --context generatorTps=50 \
  --context generatorAccounts=200 \
  --context rdsInstanceClass=db.t4g.medium \
  --context mskInstanceType=kafka.m5.large \
  --context sagemakerInstanceType=ml.t2.medium
```

---

## Changing the demo scenario at runtime

You do not need to redeploy any CDK stack to switch fraud scenario. The generator reads its configuration entirely from ECS task environment variables. The cleanest way to change scenario is to register a new task definition revision with the updated variable, then force a new deployment.

### Scenario descriptions

| Scenario | Persona distribution | Primary observable signals |
|---|---|---|
| `normal` | 100% NormalTrader | No alerts. Clean baseline dashboard. Use this to establish the baseline before introducing fraud. |
| `coordinated_ring` | 70% Normal, 30% CoordinatedRingMember | Trades on same instrument and direction within 300ms windows. Shared IP /24 subnet. Correlated P&L curves. |
| `ato_attack` | 85% Normal, 15% ATOAttacker | Burst of failed logins from new country/device, followed by success. Impossible geo-velocity. Immediate PW reset + withdrawal. |
| `registration_burst` | 60% Normal, 25% AbusiveRegistrant, 15% SystemAbuser | Multiple accounts from same IP block in 24h. Shared canvas fingerprint. Disposable email domains. Immediate bonus claim + withdrawal. |
| `system_abuse` | 70% Normal, 30% SystemAbuser | High API call rate (>200/min per account). Order/cancel ratio >80%. Quote stuffing bursts. |
| `mixed` | All 5 personas at default ratios | All typologies active simultaneously. Best for full demo. |

### How to change the scenario

**Step 1** - Get the current task definition and register a new revision with the updated variable:

```bash
NEW_SCENARIO=ato_attack   # change to desired scenario
NEW_TPS=100               # optionally adjust TPS

# Get current task definition
CURRENT_TD=$(aws ecs describe-services \
  --cluster fraud-demo-datagen \
  --services fraud-demo-datagen \
  --region $AWS_REGION \
  --query "services[0].taskDefinition" --output text)

# Write a modified copy with the new SCENARIO and TPS values
aws ecs describe-task-definition \
  --task-definition $CURRENT_TD \
  --region $AWS_REGION \
  --query "taskDefinition" \
  | python3 -c "
import sys, json
td = json.load(sys.stdin)
for c in td['containerDefinitions']:
    for env in c['environment']:
        if env['name'] == 'SCENARIO': env['value'] = '$NEW_SCENARIO'
        if env['name'] == 'TPS':      env['value'] = '$NEW_TPS'
# Remove read-only fields before re-registering
for key in ['taskDefinitionArn','revision','status','requiresAttributes',
            'compatibilities','registeredAt','registeredBy']:
    td.pop(key, None)
print(json.dumps(td))
" > /tmp/new_td.json

NEW_TD_ARN=$(aws ecs register-task-definition \
  --region $AWS_REGION \
  --cli-input-json file:///tmp/new_td.json \
  --query "taskDefinition.taskDefinitionArn" \
  --output text)

echo "New task definition: $NEW_TD_ARN"
```

**Step 2** - Update the service to use the new task definition and force a fresh deployment:

```bash
aws ecs update-service \
  --cluster fraud-demo-datagen \
  --service fraud-demo-datagen \
  --task-definition $NEW_TD_ARN \
  --force-new-deployment \
  --region $AWS_REGION

echo "Service update issued. New task will start in ~30 seconds."
echo "Watch logs: aws logs tail /ecs/fraud-demo-datagen --follow --region $AWS_REGION"
```

The old task drains and stops; the new task starts with the updated scenario. There is a 30-60 second gap in event production during the transition.

---

## Monitoring and observability

### Key CloudWatch log groups

| Log group | Service | What to look for |
|---|---|---|
| `/ecs/fraud-demo-datagen` | ECS data generator | `Sent N events | actual TPS` every 10s; any Python exceptions |
| `/aws/flink/fraud-demo-coordinated-trading` | Flink | `RUNNING`, `numRecords`, Kafka offset lag messages |
| `/aws/flink/fraud-demo-system-abuse` | Flink | Same as above |
| `/aws/flink/fraud-demo-account-takeover` | Flink | Same as above |
| `/aws/lambda/fraud-demo-alert-processor` | Lambda | `Alert <id> processed: <severity>` lines; any errors |
| `/aws/msk/fraud-demo` | MSK | Broker-level errors, replication events |
| `/aws/states/fraud-demo-investigation` | Step Functions | Execution failures only (ERROR level) |

### Key CloudWatch metrics to watch during demo

**Data throughput:**
```bash
# Check MSK bytes-in rate (events flowing from generator into Kafka)
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kafka \
  --metric-name BytesInPerSec \
  --dimensions Name=Cluster\ Name,Value=fraud-demo-cluster \
  --start-time $(date -u --date='5 minutes ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 60 --statistics Sum \
  --region $AWS_REGION \
  --query "Datapoints[*].Sum" --output text
```

**Alert production rate:**
```bash
# Check how many alert records Flink is emitting to alerts.fraud per second
aws cloudwatch get-metric-statistics \
  --namespace AWS/KinesisAnalytics \
  --metric-name numRecordsOutPerSecond \
  --dimensions Name=Application,Value=fraud-demo-account-takeover \
  --start-time $(date -u --date='5 minutes ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 60 --statistics Average \
  --region $AWS_REGION \
  --query "Datapoints[*].Average" --output text
```

**SageMaker endpoint latency:**
```bash
# Confirm ML inference is within acceptable bounds (target <200ms)
for EP in fraud-demo-coordinated-trading fraud-demo-login-risk fraud-demo-registration-anomaly; do
  echo -n "$EP model latency (us): "
  aws cloudwatch get-metric-statistics \
    --namespace AWS/SageMaker \
    --metric-name ModelLatency \
    --dimensions Name=EndpointName,Value=$EP Name=VariantName,Value=primary \
    --start-time $(date -u --date='5 minutes ago' +%Y-%m-%dT%H:%M:%SZ) \
    --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
    --period 300 --statistics Average \
    --region $AWS_REGION \
    --query "Datapoints[0].Average" --output text
done
```

### OpenSearch Dashboards quick-setup for the demo

After logging in to Dashboards and creating the index patterns (Step 8.4), build a minimal live-view dashboard:

**Alerts over time panel:**
1. Visualize -> Create visualization -> Area
2. Index: `fraud-alerts-*`
3. Y-axis: Count
4. X-axis: Date histogram on `timestamp`, interval: auto
5. Split series: Terms on `typology.keyword`
6. Save as: `Alert Volume by Typology`

**Severity breakdown panel:**
1. Visualize -> Create visualization -> Pie
2. Index: `fraud-alerts-*`
3. Slice by: Terms on `severity.keyword`, size: 4
4. Save as: `Alerts by Severity`

**Live alert feed panel:**
1. Visualize -> Create visualization -> Data table
2. Index: `fraud-alerts-*`
3. Sort by `timestamp` descending
4. Add columns: `alert_id`, `entity_id`, `typology`, `severity`, `risk_score`, `detection_method`, `signals`
5. Save as: `Live Alert Feed`

**Combine into a dashboard:**
1. Dashboard -> Create new dashboard
2. Add all three saved visualizations
3. Set time range to Last 15 minutes
4. Enable auto-refresh: 10 seconds
5. Save as: `Fraud Detection Overview`

---

## Cost breakdown

All pricing is approximate for `us-east-1` as of June 2026. Actual costs depend on data volume and exact usage patterns.

| Service | Resource | Hourly cost |
|---|---|---|
| Amazon MSK | 3 x `kafka.m5.large` brokers | $0.96 |
| Amazon RDS | 1 x `db.r5.large` PostgreSQL | $0.24 |
| Amazon Neptune | 1 x `db.r5.large` instance | $0.35 |
| Amazon OpenSearch | 2 x `m5.large.search` data nodes | $0.28 |
| Amazon Managed Flink | 8 KPUs total (3 apps combined) | $1.12 |
| Amazon SageMaker | 3 x `ml.m5.large` endpoints | $0.30 |
| Amazon ECS Fargate | 1 vCPU / 2 GB memory | $0.05 |
| Amazon NAT Gateways | 2 gateways + data transfer | ~$0.09 |
| Amazon DynamoDB | On-demand, low write volume | ~$0.02 |
| Amazon S3 | <1 GB storage + requests | ~$0.01 |
| Amazon SNS / Lambda / Step Functions | Low invocation rate | ~$0.01 |
| **Total** | | **~$3.43 / hour** |

> **Important:** MSK, Neptune, and OpenSearch accrue cost even when no events are flowing. Delete the stacks or at minimum stop Flink applications between demo sessions to reduce cost.

**Cost optimisation options for extended use:**

| Change | Saves | Trade-off |
|---|---|---|
| Use `kafka.t3.small` MSK brokers | ~$0.80/hr | Limited to fewer partitions and lower throughput |
| Use `db.t4g.medium` for RDS and Neptune | ~$0.40/hr | Less RAM; acceptable for demo volumes |
| Use 1 OpenSearch node instead of 2 | ~$0.14/hr | No HA; `red` cluster status if node restarts |
| Use `ml.t2.medium` SageMaker endpoints | ~$0.18/hr | Slightly higher inference latency |
| Stop Flink apps when not presenting | ~$1.12/hr | Must restart before demo (2-4 min) |

---

## Troubleshooting

### cdk synth fails with ModuleNotFoundError

**Cause:** CDK virtual environment is not activated, or `pip install -r requirements.txt` was not run.

**Fix:**
```bash
cd cdk
source .venv/bin/activate        # Linux/macOS
# .\.venv\Scripts\Activate.ps1  # Windows
pip install -r requirements.txt
cdk synth --quiet
```

---

### FraudDemo-ML fails: ResourceNotFoundException - model not found

**Cause:** CloudFormation tried to create the SageMaker `EndpointConfig` before the `Model` resource was ready, or the `model.tar.gz` artifact is missing from S3.

**Fix ? check S3 first:**
```bash
aws s3 ls s3://${ML_BUCKET}/models/ --recursive
```
If any `model.tar.gz` is missing, run `python scripts/upload_ml_models.py` then redeploy:
```bash
cdk deploy FraudDemo-ML --require-approval never \
  --context region=$AWS_REGION --context account=$AWS_ACCOUNT_ID
```

---

### FraudDemo-Streaming fails: topic initializer Lambda times out

**Cause:** The MSK cluster was still initialising (brokers not yet ready) when the Lambda custom resource ran. MSK can take up to 20 minutes on first creation.

**Fix:** Wait for MSK to show `ACTIVE` status, then re-run the stack:
```bash
aws kafka describe-cluster \
  --cluster-arn $MSK_CLUSTER_ARN \
  --region $AWS_REGION \
  --query "ClusterInfo.State" --output text
# Wait for: ACTIVE

cdk deploy FraudDemo-Streaming --require-approval never \
  --context region=$AWS_REGION --context account=$AWS_ACCOUNT_ID
```

---

### Flink application stays in AUTOSCALING and never reaches RUNNING

**Cause:** Your account has reached the Managed Flink KPU limit (default 24 KPUs per account per region).

**Check current KPU usage:**
```bash
aws kinesisanalyticsv2 list-applications \
  --region $AWS_REGION \
  --query "ApplicationSummaries[*].{Name:ApplicationName, Status:ApplicationStatus}" \
  --output table
```

**Fix ? Option A:** Request a service quota increase via the AWS Support console.

**Fix ? Option B:** Reduce parallelism to 1 per application (uses 1 KPU each instead of 2 or 4):
```bash
for APP in fraud-demo-coordinated-trading fraud-demo-system-abuse fraud-demo-account-takeover; do
  CURRENT_VERSION=$(aws kinesisanalyticsv2 describe-application \
    --application-name $APP --region $AWS_REGION \
    --query "ApplicationDetail.ApplicationVersionId" --output text)

  aws kinesisanalyticsv2 update-application \
    --application-name $APP --region $AWS_REGION \
    --current-application-version-id $CURRENT_VERSION \
    --application-configuration-update \
    '{"FlinkApplicationConfigurationUpdate": {"ParallelismConfigurationUpdate": {"ConfigurationTypeUpdate": "CUSTOM", "ParallelismUpdate": 1, "ParallelismPerKPUUpdate": 1, "AutoScalingEnabledUpdate": false}}}'
done
```

---

### ECS data generator task stops immediately (ExitCode 1)

**Cause:** Container startup error ? most commonly an RDS, MSK, or Secrets Manager connectivity issue.

**Diagnose:**
```bash
aws logs tail "/ecs/fraud-demo-datagen" --region $AWS_REGION
```

Refer to the troubleshooting table in Step 10.3 for specific error messages and fixes.

**Most common fix (security group):**
1. Open EC2 console -> Security Groups
2. Find `fraud-demo-rds-sg`
3. Confirm Inbound rules include: Type=PostgreSQL, Port=5432, Source=`fraud-demo-ecs-sg`
4. Find `fraud-demo-msk-sg`
5. Confirm Inbound rules include: Type=Custom TCP, Port range=9092-9098, Source=`fraud-demo-ecs-sg`

---

### OpenSearch Dashboards login fails (401 Unauthorized)

**Cause:** Incorrect password, or the fine-grained access control master user was not created correctly.

**Fix ? retrieve the current generated password:**
```bash
aws secretsmanager get-secret-value \
  --secret-id "fraud-demo/opensearch/admin" \
  --region $AWS_REGION \
  --query "SecretString" --output text \
  | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print('Password:', d['password'])"
```

Use the exact password printed (it contains no special characters by design - letters and numbers only).

---

### SNS subscription email not received

**Diagnosis steps:**
```bash
# Check current subscription status
aws sns list-subscriptions-by-topic \
  --topic-arn "arn:aws:sns:${AWS_REGION}:${AWS_ACCOUNT_ID}:fraud-demo-critical-alerts" \
  --region $AWS_REGION \
  --query "Subscriptions[*].{Protocol:Protocol, Endpoint:Endpoint, Status:SubscriptionArn}" \
  --output table
```

If `SubscriptionArn` shows `PendingConfirmation`:
1. Check your spam/junk folder for an email from `no-reply@sns.amazonaws.com`
2. The subject line is `AWS Notification - Subscription Confirmation`
3. Click the `Confirm subscription` link

If the email has expired or was not received, re-subscribe manually:
```bash
aws sns subscribe \
  --topic-arn "arn:aws:sns:${AWS_REGION}:${AWS_ACCOUNT_ID}:fraud-demo-critical-alerts" \
  --protocol email \
  --notification-endpoint $ALERT_EMAIL \
  --region $AWS_REGION
```

---

### Step Functions execution fails immediately

**Cause:** The Lambda alert processor encountered an error, or the execution input does not match the expected format.

**Diagnose the failure:**
```bash
EXEC_ARN=$(aws stepfunctions list-executions \
  --state-machine-arn "arn:aws:states:${AWS_REGION}:${AWS_ACCOUNT_ID}:stateMachine:fraud-demo-investigation" \
  --max-results 1 --region $AWS_REGION \
  --query "executions[0].executionArn" --output text)

aws stepfunctions get-execution-history \
  --execution-arn $EXEC_ARN \
  --region $AWS_REGION \
  --query "events[?type=='ExecutionFailed' || type=='TaskFailed'].{Type:type, Details:executionFailedEventDetails}" \
  --output json
```

**Most common cause:** The `$.severity` path in the input does not exist when using the EventBridge target. EventBridge wraps the payload in a `$.detail` envelope. The Step Functions input becomes `$.detail.severity` not `$.severity`. This is a known CDK wiring issue ? the state machine Choice condition should read `$.detail.severity`.

---

## Tear-down

When the demo is complete, destroy all stacks to stop all AWS charges. All resources use `RemovalPolicy.DESTROY` and `auto_delete_objects=True` where applicable, so data is permanently deleted.

> **Warning:** This operation is irreversible. All RDS data, S3 objects, DynamoDB items, and OpenSearch documents will be permanently deleted.

### Full tear-down

```bash
cd cdk
source .venv/bin/activate

cdk destroy --all \
  --force \
  --region $AWS_REGION \
  --context region=$AWS_REGION \
  --context account=$AWS_ACCOUNT_ID
```

CDK destroys stacks in reverse dependency order:
```
FraudDemo-Compute
  -> FraudDemo-Alerting
    -> FraudDemo-Processing
      -> FraudDemo-ML
      -> FraudDemo-Search
      -> FraudDemo-Graph
      -> FraudDemo-Streaming
        -> FraudDemo-Storage
          -> FraudDemo-Networking
```

**Expected duration:** 25-40 minutes. Neptune and OpenSearch take the longest to delete (~15 min each).

### Verify complete clean-up

```bash
# Confirm all FraudDemo stacks are gone
aws cloudformation list-stacks \
  --region $AWS_REGION \
  --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE DELETE_IN_PROGRESS \
  --query "StackSummaries[?starts_with(StackName,'FraudDemo')].{Name:StackName, Status:StackStatus}" \
  --output table
# Expected: empty or only DELETE_IN_PROGRESS rows

# Confirm S3 buckets were cleaned up
aws s3 ls | grep "fraud-demo"
# Expected: no output

# Confirm no lingering SageMaker endpoints
aws sagemaker list-endpoints \
  --region $AWS_REGION \
  --query "Endpoints[?starts_with(EndpointName,'fraud-demo')].EndpointName" \
  --output text
# Expected: no output

# Confirm MSK cluster is gone
aws kafka list-clusters \
  --cluster-name-filter "fraud-demo" \
  --region $AWS_REGION \
  --query "ClusterInfoList[*].ClusterName" \
  --output text
# Expected: no output
```

> The **CDK bootstrap stack** (`CDKToolkit`) is **not** deleted by `cdk destroy --all`. It is shared infrastructure. Leave it in place unless you are certain no other CDK projects exist in this account/region.

### Partial tear-down (preserve networking, remove compute)

If you want to stop costs but redeploy quickly next time by keeping the VPC:

```bash
cdk destroy \
  FraudDemo-Compute \
  FraudDemo-Alerting \
  FraudDemo-Processing \
  FraudDemo-ML \
  FraudDemo-Search \
  FraudDemo-Graph \
  FraudDemo-Streaming \
  FraudDemo-Storage \
  --force \
  --region $AWS_REGION \
  --context region=$AWS_REGION \
  --context account=$AWS_ACCOUNT_ID
```

**Note on redeployment after partial tear-down:** The S3 ML artifacts bucket is deleted as part of `FraudDemo-Storage`. You will need to re-run `python scripts/upload_ml_models.py` before deploying `FraudDemo-ML` again. Steps 3 and 4 from this guide must be repeated.

---

*End of Deployment Guide ? v1.0 | June 2026*
