# Fraud Detection Demo — Self-Contained CloudFormation Deployment

Deploy the entire solution with a single CloudFormation stack. No local tooling (Node.js, Docker, Python CDK) required.

---

## How it works

The deployment is fully self-contained using two techniques:

1. **`package.py`** — a one-time script that reads all application source files from the repo, base64-encodes them, and injects them into the CloudFormation template to produce `fraud-detection-demo-packaged.yaml`.

2. **CloudFormation Custom Resources** orchestrate everything at deploy time:

| Custom Resource | What it does |
|---|---|
| `CodeBuildTrigger` | Uploads all source code to S3, triggers CodeBuild, waits for completion |
| CodeBuild project | Builds Docker image → ECR, trains 3 ML models → S3, packages 3 Flink jobs → S3 |
| `KafkaTopics` | Creates all 7 Kafka topics on MSK after the cluster is ready |
| `RdsSchema` | Runs the full database DDL on RDS PostgreSQL |
| `OpenSearchSetup` | Creates `fraud-events` and `fraud-alerts` index templates |
| `MskBrokersSSM` | Writes MSK bootstrap broker string to SSM Parameter Store |
| `FlinkStarter` | Starts all three Managed Flink applications |
| `EcsStarter` | Launches the ECS Fargate data generator task |

---

## Prerequisites

- An AWS account with `AdministratorAccess` (recommended for a demo account)
- Python 3.x and `pip` installed locally — **only needed to run `package.py` once**
- AWS CLI configured (`aws configure`) — only needed for the deploy command

---

## Step 1 — Package the template (one-time, ~10 seconds)

This step reads the source files from the repo and injects them into the template.

```bash
cd cloudformation
pip install pyyaml
python package.py
```

This produces `fraud-detection-demo-packaged.yaml`.

---

## Step 2 — Deploy

Upload via the AWS Console or CLI. The packaged template will likely exceed the 460 KB console upload limit, so use the CLI with S3:

### Option A — AWS CLI (recommended)

```bash
aws cloudformation deploy \
  --template-file cloudformation/fraud-detection-demo-packaged.yaml \
  --stack-name fraud-detection-demo \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    AlertEmail=your-email@example.com \
    RdsMasterPassword=YourPassword123! \
  --region us-east-1
```

### Option B — AWS Console (CloudShell)

```bash
# Upload template to S3 first
aws s3 cp fraud-detection-demo-packaged.yaml s3://YOUR-BUCKET/fraud-detection-demo.yaml

# Then create stack via console using the S3 URL, or:
aws cloudformation create-stack \
  --stack-name fraud-detection-demo \
  --template-url https://s3.amazonaws.com/YOUR-BUCKET/fraud-detection-demo.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameters \
    ParameterKey=AlertEmail,ParameterValue=your-email@example.com \
    ParameterKey=RdsMasterPassword,ParameterValue=YourPassword123!
```

---

## Parameters

| Parameter | Default | Description |
|---|---|---|
| **AlertEmail** | — | **Required.** Email for CRITICAL/HIGH alert notifications |
| **RdsMasterPassword** | — | **Required.** Min 12 characters |
| GeneratorScenario | `mixed` | `mixed`, `coordinated_ring`, `ato_attack`, `registration_burst`, `system_abuse` |
| GeneratorTps | `100` | Events per second (10–500) |
| GeneratorAccounts | `500` | Synthetic accounts (100–5000) |
| RdsInstanceClass | `db.r5.large` | RDS instance size |
| MskInstanceType | `kafka.m5.large` | MSK broker instance |
| NeptuneInstanceClass | `db.r5.large` | Neptune instance |
| OpenSearchInstanceType | `m5.large.search` | OpenSearch node |
| SageMakerInstanceType | `ml.m5.large` | SageMaker endpoint instance |

---

## Deployment timeline

| Phase | Duration |
|---|---|
| Networking, S3, DynamoDB, IAM | ~3 min |
| RDS, MSK, Neptune, OpenSearch | ~20–25 min (parallel) |
| CodeBuild (Docker + ML + Flink) | ~15–20 min |
| SageMaker endpoints | ~8 min |
| Custom Resource bootstrapping | ~5 min |
| **Total** | **~55–70 min** |

---

## Post-deployment

After the stack reaches `CREATE_COMPLETE`:

1. **Confirm the SNS email subscription** — check your inbox for the AWS notification email and click Confirm.

2. **Access OpenSearch Dashboards** (VPC-private — use SSM port-forwarding or a bastion):
   ```
   URL:      https://<OpenSearchEndpoint>/_dashboards
   Username: fraud_admin
   Password: retrieve from Secrets Manager → fraud-demo/opensearch/admin
   ```

3. **Check Flink applications** — in the AWS Console → Managed Apache Flink, all three apps should be in `RUNNING` state. If any shows `READY`, start it manually.

4. **Verify data flow:**
   - ECS → ECS Cluster `fraud-demo-datagen` → task should be `RUNNING`
   - MSK → Kafka topics created (check via console or `kafka-topics.sh`)
   - Flink → CloudWatch log groups `/aws/flink/fraud-demo-*`
   - Alerts → DynamoDB `fraud-demo-alerts` table should accumulate records

---

## Cost estimate

~$10–16/hour while running:
- MSK 3-broker cluster: ~$4/hr
- OpenSearch 2-node: ~$3/hr  
- Neptune: ~$2/hr
- SageMaker 3× endpoints: ~$1.50/hr
- ECS Fargate + NAT Gateways: ~$1–2/hr

**Remember to delete the stack when done**: `aws cloudformation delete-stack --stack-name fraud-detection-demo`

---

## Architecture diagram

```
CloudFormation Stack
       │
       ├── CodeBuild ──builds──> ECR (Docker image)
       │          └──uploads──> S3 (ML model.tar.gz × 3, Flink job.zip × 3)
       │
       ├── ECS Fargate (data generator)
       │       │ generates synthetic trades/sessions/registrations
       │       ▼
       ├── Amazon MSK (Kafka 3.6, 3 brokers, IAM auth)
       │       │ trades.raw / sessions.raw / registrations.raw / api.calls
       │       ▼
       ├── Managed Apache Flink (3 apps)
       │       │ Rules + ML (SageMaker) + Graph (Neptune)
       │       ▼ alerts.fraud
       ├── EventBridge (fraud-detection-demo bus)
       │       ├──> SNS (email for CRITICAL/HIGH)
       │       ├──> Lambda (persist to DynamoDB + OpenSearch)
       │       └──> Step Functions (triage: AutoBlock / FlagForReview / Monitor)
       │
       └── OpenSearch Dashboards (investigation UI)
```
