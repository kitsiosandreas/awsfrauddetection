"""
S3 seeder — uploads enrichment reference data to the enrichment bucket.
Also archives baseline historical trade data as Parquet for ML training.
"""
import json
import io
import logging
import random
from datetime import datetime, timezone

import boto3

logger = logging.getLogger(__name__)

# Simulated high-risk IP prefix list (would come from threat intel in production)
HIGH_RISK_IP_PREFIXES = [
    "41.58",    # Nigeria
    "197.210",  # Nigeria
    "113.160",  # Vietnam
    "14.161",   # Vietnam
    "95.24",    # Russia
    "188.130",  # Russia
]

# GeoIP reference (simplified country→lat/lon lookup)
GEOIP_SAMPLE = [
    {"ip_prefix": "203.12",  "country": "AU", "city": "Sydney",     "lat": -33.87, "lon": 151.21},
    {"ip_prefix": "81.149",  "country": "GB", "city": "London",     "lat":  51.51, "lon":  -0.13},
    {"ip_prefix": "85.214",  "country": "DE", "city": "Frankfurt",  "lat":  50.11, "lon":   8.68},
    {"ip_prefix": "103.10",  "country": "SG", "city": "Singapore",  "lat":   1.35, "lon": 103.82},
    {"ip_prefix": "72.21",   "country": "US", "city": "New York",   "lat":  40.71, "lon": -74.01},
    {"ip_prefix": "126.",    "country": "JP", "city": "Tokyo",      "lat":  35.68, "lon": 139.69},
    {"ip_prefix": "41.58",   "country": "NG", "city": "Lagos",      "lat":   6.52, "lon":   3.38, "risk": 0.9},
    {"ip_prefix": "95.24",   "country": "RU", "city": "Moscow",     "lat":  55.75, "lon":  37.62, "risk": 0.8},
    {"ip_prefix": "113.160", "country": "VN", "city": "Ho Chi Minh","lat":  10.82, "lon": 106.63, "risk": 0.75},
]


def upload_enrichment_data(bucket_name: str, region: str) -> None:
    """Upload reference data files to the enrichment S3 bucket."""
    if not bucket_name:
        logger.warning("RAW_DATA_BUCKET not set — skipping S3 seed")
        return

    s3 = boto3.client("s3", region_name=region)
    logger.info(f"Uploading enrichment data to s3://{bucket_name}/")

    # ── GeoIP reference ───────────────────────────────────────────────────
    _upload_json(s3, bucket_name,
                 "enrichment/geoip/geoip_sample.json", GEOIP_SAMPLE)

    # ── High-risk IP list ─────────────────────────────────────────────────
    risk_list = [
        {"ip_prefix": p, "risk_score": round(random.uniform(0.7, 0.99), 2)}
        for p in HIGH_RISK_IP_PREFIXES
    ]
    _upload_json(s3, bucket_name,
                 "enrichment/ip_reputation/high_risk_prefixes.json", risk_list)

    # ── Disposable email domains ──────────────────────────────────────────
    disposable = {
        "domains": [
            "mailnull.com", "trashmail.com", "guerrillamail.com",
            "tempmail.com", "throwam.com", "yopmail.com",
            "sharklasers.com", "dispostable.com", "spam4.me",
        ]
    }
    _upload_json(s3, bucket_name,
                 "enrichment/email/disposable_domains.json", disposable)

    logger.info("S3 enrichment data upload complete.")


def _upload_json(s3_client, bucket: str, key: str, data) -> None:
    body = json.dumps(data, default=str, indent=2).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    logger.debug(f"  Uploaded s3://{bucket}/{key}")
