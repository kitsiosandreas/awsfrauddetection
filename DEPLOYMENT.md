# Fraud Detection Demo — QuickSight Edition: Deployment Guide

A complete, step-by-step guide to deploying the QuickSight package. Everything
needed lives in this folder (`cloudformation-quicksight/`).

---

## How the files map to the deployment

The key thing to understand: **the deployer only ever touches one file** —
`fraud-detection-quicksight-packaged.yaml`. Everything else either got baked
into it or is a developer build input.

| File | Deployed? | How it gets into AWS |
|---|---|---|
| `fraud-detection-quicksight-packaged.yaml` | **Yes — this is the one you deploy** | Uploaded to CloudFormation (console or CLI). Creates all infrastructure. |
| `source_extracted/**` (data generator) | Indirectly | Already zipped + base64-embedded inside the packaged template. At deploy time the `SourceUploader` Lambda writes it to `s3://<BootstrapBucket>/buildspec/source.zip`. CodeBuild then pulls it, builds the Docker image, pushes to ECR, and launches the ECS task. |
| `fraud-detection-quicksight.yaml` | No | Source template — input to the packager only. |
| `package_quicksight.py` | No | Build tool that produced the packaged template. Run only when you change the source. |
| `README.md` / `DEPLOYMENT.md` | No | Documentation. |

Deploy-time file flow:

```
fraud-detection-quicksight-packaged.yaml
   │  (deploy)
   ├── creates all infra (VPC, Kinesis, RDS, Lambdas, Firehose, Glue, Athena, QuickSight, ECS, CodeBuild…)
   └── SourceUploader Lambda ──► s3://<BootstrapBucket>/buildspec/source.zip
                                          │
        (you run the bootstrap)           ▼
   CodeBuild "fraud-demo-bootstrap" ── pulls source.zip
        ├── docker build (public.ecr.aws base image) ──► push to ECR
        └── start_datagen.py ──► launches ECS Fargate task (data generator)
```

---

## Phase 0 — Prerequisites (one-time)

1. **AWS account** with `AdministratorAccess` (a demo/sandbox account is ideal).

2. **Pick your region.** The stack is region-agnostic, but it must be a region
   that offers all the services (Kinesis, Firehose, RDS, Glue, Athena, Step
   Functions, ECS Fargate, CodeBuild, and **QuickSight**). Major regions like
   `us-east-1`, `us-west-2`, `eu-west-1`, `ap-southeast-2` are safe.

3. **Enable Amazon QuickSight in that same region** (this is the one step
   CloudFormation cannot do for you):
   - AWS Console → QuickSight → **Sign up for QuickSight**
   - Choose **Enterprise** edition (required for SPICE + Athena).
   - Make sure the selected region matches where you'll deploy.

4. **Find your QuickSight username** — you'll pass it as a parameter. Either
   read it on the QuickSight console (top-right → your profile), or:
   ```bash
   aws quicksight list-users \
     --aws-account-id <YOUR_ACCOUNT_ID> \
     --namespace default \
     --region <YOUR_REGION> \
     --query "UserList[].UserName" --output text
   ```
   Use just the username portion (e.g. `admin`, not the full ARN).

---

## Phase 1 — Get the template into CloudShell

Open **AWS CloudShell** in your target region (icon in the console top bar).
Then get the single packaged template into the session using any one of these:

- **Upload:** CloudShell → **Actions → Upload file** → select
  `fraud-detection-quicksight-packaged.yaml`.
- **Or clone** if the repo is in git:
  ```bash
  git clone <your-repo-url>
  cd <repo>/cloudformation-quicksight
  ```

Confirm it's there:
```bash
ls -lh fraud-detection-quicksight-packaged.yaml
```

---

## Phase 2 — Deploy the stack

The template is ~124 KB (under the 460 KB console limit), so no S3 staging is
needed. From CloudShell:

```bash
aws cloudformation deploy \
  --template-file fraud-detection-quicksight-packaged.yaml \
  --stack-name fraud-detection-quicksight \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    AlertEmail=you@example.com \
    RdsMasterPassword='ChooseAStrongPassword123!' \
    QuickSightUserName=your-qs-username
```

CloudShell already targets its own region; add `--region <your-region>` only if
you want to override it.

Parameters:

| Parameter | Required | Default | Notes |
|---|---|---|---|
| `AlertEmail` | Yes | — | Gets CRITICAL/HIGH alert emails |
| `RdsMasterPassword` | Yes | — | Min 12 chars; quote it to protect shell special chars |
| `QuickSightUserName` | Yes | — | From Phase 0 step 4 |
| `EnvironmentName` | No | `fraud-demo` | Prefix for all resource names |
| `GeneratorScenario` | No | `mixed` | `mixed`, `coordinated_ring`, `ato_attack`, `registration_burst`, `system_abuse`, `normal` |
| `GeneratorTps` | No | `100` | 10–500 |
| `GeneratorAccounts` | No | `500` | 100–5000 |
| `RdsInstanceClass` | No | `db.r5.large` | — |

This takes roughly 20–30 minutes (RDS is the long pole). Wait for
`CREATE_COMPLETE`:
```bash
aws cloudformation wait stack-create-complete --stack-name fraud-detection-quicksight
```

> **Alternative (console):** CloudFormation → Create stack → Upload a template
> file → choose the same `.yaml`, fill the parameters, check the IAM capability
> box, Create.

---

## Phase 3 — Confirm the alert email

Check the inbox for `AlertEmail` and click **Confirm subscription** in the AWS
notification. Until you confirm, no alert emails will be delivered.

---

## Phase 4 — Run the one-time bootstrap

This builds the data-generator Docker image and launches the ECS task. It's
intentionally a separate, manual step so the fragile Docker build never risks
rolling back your infrastructure.

```bash
aws codebuild start-build --project-name fraud-demo-bootstrap
```

(If `EnvironmentName` wasn't the default, the project is
`<EnvironmentName>-bootstrap`. The exact command is also in the stack's
`BootstrapCommand` output.)

Watch it (takes ~10 min):
```bash
# Get the latest build id
BUILD_ID=$(aws codebuild list-builds-for-project --project-name fraud-demo-bootstrap \
  --query "ids[0]" --output text)

# Poll status until SUCCEEDED
aws codebuild batch-get-builds --ids "$BUILD_ID" \
  --query "builds[0].buildStatus" --output text
```

When it reports `SUCCEEDED`, confirm the generator is running:
```bash
aws ecs list-tasks --cluster fraud-demo-datagen --query "taskArns" --output text
```

---

## Phase 5 — Let data flow and the catalog populate

Once the ECS task runs, events flow through Kinesis → Lambda detectors →
alerts → Firehose → S3 (Parquet). Glue crawlers run every 15 minutes to
register partitions in Athena.

To avoid waiting, trigger the crawlers immediately after a few minutes of data:
```bash
aws glue start-crawler --name fraud-demo-alerts-crawler
aws glue start-crawler --name fraud-demo-trades-crawler
```

Verify Athena can see the tables:
```bash
aws glue get-tables --database-name fraud-demo_fraud_analytics \
  --query "TableList[].Name" --output text
# expect: alerts  trades
```

---

## Phase 6 — Light up the QuickSight dashboards

The stack pre-creates the Athena data source and four datasets:
- `fraud-demo-alerts-overview-ds` (SPICE)
- `fraud-demo-entity-risk-ds` (SPICE)
- `fraud-demo-trade-activity-ds` (SPICE)
- `fraud-demo-alert-feed-ds` (Direct Query — always live)

Steps:
1. **Grant QuickSight access to Athena/S3** (one-time, if not already):
   QuickSight → top-right → **Manage QuickSight → Security & permissions →
   QuickSight access to AWS services** → enable **Amazon Athena** and the
   **analytics S3 bucket** (`fraud-demo-analytics-<account>-<region>`).
2. **Refresh the three SPICE datasets:** QuickSight → **Datasets** → open each
   → **Refresh now**. (The Alert Feed dataset needs no refresh.)
3. **Build analyses:** Datasets → select one → **Create analysis**. Suggested
   visuals:

   | Dataset | Suggested visuals |
   |---|---|
   | Alerts Overview | Line: alerts/day by severity; Donut: by typology; KPI: total/critical/avg risk |
   | Entity Risk Monitor | Table: top entities by risk; Scatter: risk vs alert count |
   | Alert Feed (Live) | Filterable table with severity/typology/date filters |
   | Trade Activity | Bar: volume by instrument; Line: trades over time colored by label |

The Athena workgroup `fraud-demo-fraud-analytics` also has pre-built named
queries you can reference.

---

## Phase 7 — Verification checklist

```bash
# Stack status
aws cloudformation describe-stacks --stack-name fraud-detection-quicksight \
  --query "Stacks[0].StackStatus" --output text          # CREATE_COMPLETE

# Useful outputs (bucket names, commands, dataset notes)
aws cloudformation describe-stacks --stack-name fraud-detection-quicksight \
  --query "Stacks[0].Outputs" --output table

# Data landing in S3 (Parquet partitions)
aws s3 ls s3://fraud-demo-analytics-<account>-<region>/alerts/ --recursive | head

# Alerts being persisted
aws dynamodb scan --table-name fraud-demo-alerts --select COUNT --query "Count"
```

---

## Phase 8 — Cleanup

```bash
aws cloudformation delete-stack --stack-name fraud-detection-quicksight
aws cloudformation wait stack-delete-complete --stack-name fraud-detection-quicksight
```

S3 buckets, DynamoDB tables, and the ECR repo use `DeletionPolicy: Delete`, so
they're removed with the stack. Two manual notes:
- The QuickSight datasets/data source are stack-managed and will be deleted;
  **QuickSight itself stays enabled** (and keeps billing ~$24/mo for the
  author) until you unsubscribe in the QuickSight console.
- If you redeploy soon after deleting, the Secrets Manager secret
  `fraud-demo/rds/credentials` may still be in its recovery window — either
  wait, or force-delete it.

---

## Regenerating the template (developers only)

You only need this if you change the data generator or the source template:
```bash
cd cloudformation-quicksight
pip install pyyaml          # only needed for the optional YAML validation step
python package_quicksight.py
# → rewrites fraud-detection-quicksight-packaged.yaml
```

---

## Quick reference — resource names (with default `EnvironmentName=fraud-demo`)

| Thing | Name |
|---|---|
| CloudFormation stack | `fraud-detection-quicksight` (your choice) |
| Bootstrap CodeBuild project | `fraud-demo-bootstrap` |
| ECS cluster | `fraud-demo-datagen` |
| Glue database | `fraud-demo_fraud_analytics` |
| Glue crawlers | `fraud-demo-alerts-crawler`, `fraud-demo-trades-crawler` |
| Athena workgroup | `fraud-demo-fraud-analytics` |
| Analytics S3 bucket | `fraud-demo-analytics-<account>-<region>` |
| DynamoDB alerts table | `fraud-demo-alerts` |
| RDS secret | `fraud-demo/rds/credentials` |
