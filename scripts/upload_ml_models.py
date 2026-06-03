"""
Script — Train ML models locally and upload artifacts to S3.
Run this BEFORE cdk deploy so SageMaker models can be created.

Usage:
    python scripts/upload_ml_models.py --region us-east-1 --bucket fraud-demo-ml-<account>-<region>
"""
import argparse
import logging
import os
import subprocess
import sys
import tempfile
import boto3

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

MODELS = [
    {
        "name":        "coordinated_trading",
        "train_script": "ml_models/coordinated_trading/train.py",
        "s3_prefix":   "models/coordinated_trading",
    },
    {
        "name":        "registration_anomaly",
        "train_script": "ml_models/registration_anomaly/train.py",
        "s3_prefix":   "models/registration_anomaly",
    },
    {
        "name":        "login_risk",
        "train_script": "ml_models/login_risk/train.py",
        "s3_prefix":   "models/login_risk",
    },
]


def train_and_package(train_script: str, model_dir: str, output_dir: str) -> str:
    """Run training script and return path to output model.tar.gz."""
    logger.info(f"Training {train_script}...")
    result = subprocess.run(
        [sys.executable, train_script,
         "--model-dir", model_dir,
         "--output-dir", output_dir],
        capture_output=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Training failed for {train_script}")
    tar_path = os.path.join(output_dir, "model.tar.gz")
    if not os.path.exists(tar_path):
        raise FileNotFoundError(f"Expected model.tar.gz at {tar_path}")
    return tar_path


def upload_to_s3(local_path: str, bucket: str, s3_key: str, region: str) -> str:
    s3 = boto3.client("s3", region_name=region)
    logger.info(f"Uploading {local_path} → s3://{bucket}/{s3_key}")
    s3.upload_file(local_path, bucket, s3_key)
    return f"s3://{bucket}/{s3_key}"


def resolve_bucket(args) -> str:
    if args.bucket:
        return args.bucket
    # Try to resolve from CloudFormation stack output
    cf = boto3.client("cloudformation", region_name=args.region)
    stacks = cf.describe_stacks(StackName="FraudDemo-Storage")
    outputs = {o["OutputKey"]: o["OutputValue"]
               for o in stacks["Stacks"][0].get("Outputs", [])}
    bucket = outputs.get("MlArtifactsBucket")
    if not bucket:
        raise ValueError("Could not resolve ML artifacts bucket. Pass --bucket explicitly.")
    return bucket


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--bucket", default=None, help="ML artifacts S3 bucket name")
    parser.add_argument("--skip-train", action="store_true",
                        help="Skip local training (assume artifacts already in /tmp)")
    args = parser.parse_args()

    bucket = resolve_bucket(args)
    logger.info(f"Target bucket: s3://{bucket}")

    # Ensure we run from repo root
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(repo_root)

    for model in MODELS:
        with tempfile.TemporaryDirectory() as tmp:
            model_dir  = os.path.join(tmp, "model")
            output_dir = os.path.join(tmp, "output")
            os.makedirs(model_dir, exist_ok=True)
            os.makedirs(output_dir, exist_ok=True)

            if not args.skip_train:
                tar_path = train_and_package(
                    model["train_script"], model_dir, output_dir
                )
            else:
                tar_path = os.path.join(output_dir, "model.tar.gz")

            s3_key = f"{model['s3_prefix']}/model.tar.gz"
            upload_to_s3(tar_path, bucket, s3_key, args.region)

    logger.info("All models uploaded. You can now run 'cdk deploy FraudDemo-ML'.")


if __name__ == "__main__":
    main()
