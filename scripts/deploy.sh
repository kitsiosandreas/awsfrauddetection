#!/usr/bin/env bash
# ============================================================
# Fraud Detection Demo — Full Deployment Script
# Usage: ./scripts/deploy.sh [--region us-east-1] [--email you@example.com]
# ============================================================
set -euo pipefail

REGION="${REGION:-us-east-1}"
ALERT_EMAIL="${ALERT_EMAIL:-fraud-demo-alerts@example.com}"
RDS_PASSWORD="${RDS_PASSWORD:-FraudDemo2024!}"

# Parse optional args
while [[ $# -gt 0 ]]; do
  case $1 in
    --region)  REGION="$2";      shift 2 ;;
    --email)   ALERT_EMAIL="$2"; shift 2 ;;
    *)         shift ;;
  esac
done

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Fraud Detection Demo Deployment"
echo "  Region:      $REGION"
echo "  Alert email: $ALERT_EMAIL"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# 1. Train and upload ML models (requires Python + sklearn)
echo "[1/6] Training and uploading ML models..."
python scripts/upload_ml_models.py --region "$REGION" || {
  echo "  ⚠ ML upload failed — models must be uploaded before FraudDemo-ML deploys"
}

# 2. CDK bootstrap (idempotent)
echo "[2/6] CDK bootstrap..."
cd cdk
pip install -r requirements.txt -q
cdk bootstrap "aws://$(aws sts get-caller-identity --query Account --output text)/$REGION" \
  --region "$REGION"

# 3. Deploy all stacks
echo "[3/6] Deploying CDK stacks..."
cdk deploy --all \
  --require-approval never \
  --region "$REGION" \
  --context "region=$REGION" \
  --context "alertEmail=$ALERT_EMAIL" \
  -c "RdsMasterPassword=$RDS_PASSWORD"
cd ..

# 4. Set up OpenSearch indices
echo "[4/6] Setting up OpenSearch indices..."
python scripts/setup_opensearch.py --region "$REGION" || \
  echo "  ⚠ OpenSearch setup failed — run manually after deploy"

# 5. Confirm Flink applications are RUNNING
echo "[5/6] Starting Flink applications..."
for app in fraud-demo-coordinated-trading fraud-demo-system-abuse fraud-demo-account-takeover; do
  STATUS=$(aws kinesisanalyticsv2 describe-application \
    --application-name "$app" \
    --region "$REGION" \
    --query "ApplicationDetail.ApplicationStatus" \
    --output text 2>/dev/null || echo "NOT_FOUND")

  if [[ "$STATUS" == "READY" ]]; then
    aws kinesisanalyticsv2 start-application \
      --application-name "$app" \
      --run-configuration '{}' \
      --region "$REGION"
    echo "  Started: $app"
  elif [[ "$STATUS" == "RUNNING" ]]; then
    echo "  Already running: $app"
  else
    echo "  ⚠ $app status: $STATUS — start manually"
  fi
done

# 6. Summarise outputs
echo "[6/6] Deployment outputs:"
aws cloudformation describe-stacks \
  --region "$REGION" \
  --query 'Stacks[?starts_with(StackName,`FraudDemo`)].{Stack:StackName,Outputs:Outputs[*].{Key:OutputKey,Val:OutputValue}}' \
  --output table 2>/dev/null || true

echo ""
echo "✓ Deployment complete."
echo "  Dashboard: https://\$(OpenSearchEndpoint)/_dashboards"
echo "  Data generator starts automatically via ECS."
