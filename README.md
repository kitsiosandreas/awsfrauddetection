# Fraud Detection Demo — QuickSight Edition

Replaces OpenSearch Dashboards with **Amazon QuickSight** as the visualization layer, reducing cost by ~$3/hr while providing a fully managed, shareable BI dashboard for fraud monitoring.

---

## Architecture

```
ECS Fargate (data generator)
        │
        ├──► Kinesis: trades / sessions / registrations / api-calls
        │          │
        │          └──► Lambda Detectors (heuristics)
        │                      │
        │                      └──► Kinesis: alerts
        │                                  │
        │                    ┌─────────────┴────────────┐
        │                    ▼                          ▼
        │           Lambda AlertProcessor          Firehose (alerts)
        │                 │   │                       │
        │           DynamoDB  SNS (email)             │
        │           (triage)                          │
        │                 │                           ▼
        │          Step Functions          S3 Analytics Bucket
        │          (auto-block /         /alerts/year=.../month=.../day=.../
        │           flag / monitor)
        │
        └──► Kinesis: trades ──► Firehose (trades) ──► S3 Analytics Bucket
                                                      /trades/year=.../month=.../day=.../

S3 Analytics Bucket
    └──► Glue Crawlers (every 15 min) ──► Glue Data Catalog
                                              └──► Athena Workgroup
                                                       └──► QuickSight
                                                            ├── Fraud Overview (SPICE)
                                                            ├── Entity Risk Monitor (SPICE)
                                                            ├── Alert Feed (Direct Query)
                                                            └── Trade Activity (SPICE)
```

### What changed from the original

| Removed | Replaced with |
|---|---|
| Amazon OpenSearch Service (`m5.large.search`, 100 GB EBS) | — |
| OpenSearch indexing in `AlertProcessorLambda` | Kinesis Data Firehose (automatic, no Lambda code) |
| OpenSearch Dashboards (VPC-private, SigV4 auth) | Amazon QuickSight (SaaS, no VPC, shareable links) |

---

## Region support

The stack is **region-agnostic**. Every resource derives its region and
partition from the CloudFormation pseudo-parameters (`AWS::Region`,
`AWS::Partition`, `AWS::AccountId`), so it deploys unchanged into any region —
or partition — where the underlying services are available.

Just deploy from a CloudShell session opened in your target region (or pass
`--region <your-region>` to the AWS CLI). The whole demo runs there.

Required services (confirm availability in your chosen region): Kinesis Data
Streams, Kinesis Data Firehose, Lambda, ECS Fargate, ECR, CodeBuild, RDS
PostgreSQL, DynamoDB, Glue, Athena, Step Functions, SNS, and QuickSight.

---

## Pre-requisites

1. **AWS account** with `AdministratorAccess` (demo account recommended)
2. **Amazon QuickSight must be enabled** in the account/region — this is a
   one-time manual action that cannot be automated via CloudFormation:
   - Go to the AWS Console → QuickSight → Sign up for QuickSight
   - Choose **Enterprise** edition (required for SPICE + Athena integration)
   - Make sure QuickSight is enabled in the **same region** you deploy into.
   - Note the **QuickSight username** of the admin user (shown on the
     QuickSight account settings page). You'll need this for the parameter.
3. **AWS CloudShell** (or any shell with the AWS CLI configured). Nothing else
   is required — no Node.js, Docker, CDK, or Python on your machine.

> The `package_quicksight.py` script and the `source_extracted/` tree are
> developer build inputs only. The deployer needs just the single packaged
> template file produced below.

---

## Step 1 — Get the packaged template

Download (or copy into CloudShell) the single self-contained file:

```
fraud-detection-quicksight-packaged.yaml
```

It already embeds the data-generator source and a `SourceUploader` custom
resource that uploads it to S3 at deploy time — so there is **no manual S3
upload and no separate packaging step** for the deployer.

> Regenerating the template (developers only): `pip install pyyaml &&
> python package_quicksight.py`.

---

## Step 2 — Deploy

The packaged template is ~124 KB — under the 460 KB console limit — so you can
upload it directly in the CloudFormation console, or deploy it from CloudShell
with one command (it uses the region of your current session):

```bash
aws cloudformation deploy \
  --template-file fraud-detection-quicksight-packaged.yaml \
  --stack-name fraud-detection-quicksight \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    AlertEmail=your-email@example.com \
    RdsMasterPassword=YourPassword123! \
    QuickSightUserName=your-qs-username
```

To target a specific region, add `--region <your-region>` (otherwise the CLI
uses the region configured in your session).

---

## Parameters

| Parameter | Default | Description |
|---|---|---|
| **AlertEmail** | — | **Required.** Email for CRITICAL/HIGH alert notifications |
| **RdsMasterPassword** | — | **Required.** Min 12 characters |
| **QuickSightUserName** | — | **Required.** Your QuickSight username (the username portion only, e.g. `admin`) |
| GeneratorScenario | `mixed` | `mixed`, `coordinated_ring`, `ato_attack`, `registration_burst`, `system_abuse` |
| GeneratorTps | `100` | Events per second (10–500) |
| GeneratorAccounts | `500` | Synthetic accounts (100–5000) |
| RdsInstanceClass | `db.r5.large` | RDS instance size |

---

## Step 3 — Run the bootstrap (once, after CREATE_COMPLETE)

The stack outputs the exact command:

```bash
aws codebuild start-build --project-name fraud-demo-bootstrap
```

(Add `--region <your-region>` if your CLI session isn't already set to the
region you deployed into.) This builds the Docker image and starts the ECS
Fargate data generator (~10 min).

---

## Step 4 — Post-deployment

### 4a. Confirm SNS subscription
Check your inbox for the AWS notification email and click **Confirm subscription**.

### 4b. Wait for the first Glue crawler run
The Glue crawlers run on a 15-minute schedule. After the first run, the
`alerts` and `trades` tables will appear in the Glue Data Catalog and
Athena will be able to query them.

You can trigger a crawler immediately from the console:
```
AWS Console → Glue → Crawlers → fraud-demo-alerts-crawler → Run
AWS Console → Glue → Crawlers → fraud-demo-trades-crawler → Run
```

### 4c. Refresh SPICE datasets
Open **QuickSight → Datasets** and trigger a manual SPICE ingestion for
the three SPICE datasets:
- `fraud-demo Fraud Alerts Overview`
- `fraud-demo Entity Risk Monitor`
- `fraud-demo Trade Activity`

The **Alert Feed (Live)** dataset uses Direct Query and needs no refresh.

### 4d. Create dashboards
Open **QuickSight → Datasets**, select a dataset, and click **Create analysis**.
Use the pre-built Athena named queries as a reference for the recommended
visualizations:

| Dataset | Suggested visuals |
|---|---|
| Fraud Alerts Overview | Line chart: alerts/day by severity; Donut: by typology; KPI tiles: total, critical count, avg risk score |
| Entity Risk Monitor | Table: top 20 entities by risk score; Scatter: risk_score vs alert count |
| Alert Feed (Live) | Filterable table with severity, typology, date range filters |
| Trade Activity | Bar: volume by instrument; Line: trade count over time; colour by label (normal vs fraud) |

---

## Data flow timing

| Event | Latency |
|---|---|
| Trade/session event → Kinesis | < 1 second |
| Kinesis → Lambda detector → AlertsStream | 1–5 seconds |
| AlertsStream → Firehose → S3 (Parquet) | 60 seconds (buffer) |
| S3 new partition → Glue crawler → Athena visible | up to 15 minutes |
| Athena query → QuickSight SPICE refresh | on-demand or scheduled |
| QuickSight Direct Query (Alert Feed) | real-time (Athena query on page load) |

---

## Cost estimate

~$7–10/hr while running (compared to ~$10–16/hr for the original):

| Service | Cost |
|---|---|
| RDS PostgreSQL `db.r5.large` | ~$0.48/hr |
| ECS Fargate (1 vCPU, 2 GB) | ~$0.06/hr |
| Kinesis Streams (7 shards) | ~$0.21/hr |
| Kinesis Firehose (2 streams) | ~$0.01/hr |
| NAT Gateway | ~$0.045/hr + data transfer |
| Glue Crawlers (4 DPU × 15 min × 2) | ~$0.03/hr |
| S3 Analytics Bucket | ~$0.002/hr |
| Athena queries | ~$0.005/hr (SPICE refreshes) |
| **QuickSight** | **$24/month flat (1 author)** |
| ~~OpenSearch~~ | ~~$3/hr~~ → **removed** |

---

## Cleanup

```bash
aws cloudformation delete-stack --stack-name fraud-detection-quicksight
```

Note: DynamoDB tables, S3 buckets, and the ECR repository have
`DeletionPolicy: Delete` and will be removed with the stack.

---

## Troubleshooting

**Firehose not delivering to S3**
- Check the Firehose CloudWatch log group `/aws/kinesisfirehose/fraud-demo-alerts`
- Common cause: the Glue table schema doesn't match the JSON shape. Check
  that the `alerts` Glue table columns match the fields in the alert events.

**QuickSight "Insufficient permissions" on data source**
- Confirm that QuickSight has been granted access to the Athena workgroup and
  S3 bucket in the QuickSight console under **Manage QuickSight → Security & permissions**.
- The `AnalyticsBucketPolicy` in the stack grants `quicksight.amazonaws.com`
  read access — ensure this deployed cleanly.

**No data in Athena after 30 minutes**
- Verify the ECS task is running: `AWS Console → ECS → fraud-demo-datagen → Tasks`
- Verify the Firehose is consuming from Kinesis: check the Firehose monitoring
  metrics for `IncomingRecords`.
- Trigger the Glue crawler manually to force partition discovery.

**QuickSight dataset refresh fails with "Table not found"**
- The Glue crawler hasn't run yet. Trigger it manually (see Step 4b above).
