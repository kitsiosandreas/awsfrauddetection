"""
RDS seeder — creates schema and inserts account baseline data.
Run once at generator startup before streaming begins.
"""
import json
import logging
import random
from datetime import datetime, timezone, timedelta
from typing import Optional

import boto3
import psycopg2
from psycopg2.extras import execute_batch

from utils.kyc_generator import generate_identity
from utils.geo_data import pick_location
from utils.device_fingerprint import generate_fingerprint

logger = logging.getLogger(__name__)


def get_rds_credentials(secret_arn: str, region: str) -> dict:
    """Fetch RDS credentials from Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_arn)
    return json.loads(response["SecretString"])


def get_connection(config) -> psycopg2.extensions.connection:
    """Return a psycopg2 connection, resolving credentials if needed."""
    from config import Config

    if Config.RDS_SECRET_ARN:
        creds = get_rds_credentials(Config.RDS_SECRET_ARN, Config.AWS_REGION)
        user = creds["username"]
        password = creds["password"]
    else:
        user = Config.RDS_USER
        password = Config.RDS_PASSWORD

    return psycopg2.connect(
        host=Config.RDS_ENDPOINT,
        port=Config.RDS_PORT,
        dbname=Config.RDS_DB,
        user=user,
        password=password,
        connect_timeout=10,
        sslmode="require",
    )


DDL = """
-- Accounts
CREATE TABLE IF NOT EXISTS accounts (
    account_id        VARCHAR(36)    PRIMARY KEY,
    first_name        VARCHAR(100)   NOT NULL,
    last_name         VARCHAR(100)   NOT NULL,
    email             VARCHAR(255)   NOT NULL UNIQUE,
    phone             VARCHAR(50),
    nationality       CHAR(2),
    date_of_birth     DATE,
    doc_type          VARCHAR(30),
    doc_number        VARCHAR(50),
    kyc_status        VARCHAR(20)    DEFAULT 'PENDING',
    risk_tier         VARCHAR(20)    DEFAULT 'STANDARD',
    created_at        TIMESTAMPTZ    DEFAULT NOW(),
    last_login_at     TIMESTAMPTZ,
    last_login_country CHAR(2),
    last_login_city   VARCHAR(100),
    last_login_lat    DOUBLE PRECISION,
    last_login_lon    DOUBLE PRECISION,
    last_device_id    VARCHAR(36),
    balance           NUMERIC(18,2)  DEFAULT 0,
    is_active         BOOLEAN        DEFAULT TRUE,
    persona_type      VARCHAR(50),
    cluster_id        VARCHAR(36)
);

-- Login history (last 90 days of baseline)
CREATE TABLE IF NOT EXISTS login_history (
    id            BIGSERIAL        PRIMARY KEY,
    account_id    VARCHAR(36)      NOT NULL REFERENCES accounts(account_id),
    login_at      TIMESTAMPTZ      NOT NULL,
    ip_address    VARCHAR(45),
    country_code  CHAR(2),
    city          VARCHAR(100),
    lat           DOUBLE PRECISION,
    lon           DOUBLE PRECISION,
    device_id     VARCHAR(36),
    success       BOOLEAN          DEFAULT TRUE
);

-- Trade history (used for ML feature baseline)
CREATE TABLE IF NOT EXISTS trade_history (
    id            BIGSERIAL        PRIMARY KEY,
    account_id    VARCHAR(36)      NOT NULL REFERENCES accounts(account_id),
    traded_at     TIMESTAMPTZ      NOT NULL,
    instrument    VARCHAR(20),
    direction     VARCHAR(4),
    lots          NUMERIC(10,2),
    open_price    NUMERIC(18,5),
    close_price   NUMERIC(18,5),
    pnl           NUMERIC(18,2)
);

-- IP reputation reference table
CREATE TABLE IF NOT EXISTS ip_reputation (
    ip_prefix     VARCHAR(20)   PRIMARY KEY,
    risk_score    NUMERIC(4,2)  DEFAULT 0,
    country_code  CHAR(2),
    is_vpn        BOOLEAN       DEFAULT FALSE,
    is_tor        BOOLEAN       DEFAULT FALSE,
    last_updated  TIMESTAMPTZ   DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_login_history_account ON login_history(account_id, login_at DESC);
CREATE INDEX IF NOT EXISTS idx_trade_history_account ON trade_history(account_id, traded_at DESC);
"""


def seed_database(accounts: list[dict]) -> None:
    """
    Create schema and insert all accounts + 30-day baseline history.
    Safe to re-run — uses INSERT ... ON CONFLICT DO NOTHING.
    """
    logger.info(f"Seeding RDS with {len(accounts)} accounts...")
    from config import Config
    conn = get_connection(Config)
    cur = conn.cursor()

    cur.execute(DDL)
    conn.commit()

    # ── Insert accounts ───────────────────────────────────────────────────
    account_rows = []
    for a in accounts:
        kyc = a["kyc"]
        loc = a["home_location"]
        account_rows.append((
            a["account_id"],
            kyc["first_name"],
            kyc["last_name"],
            kyc["email"],
            kyc.get("phone"),
            kyc["nationality"],
            kyc["date_of_birth"],
            kyc.get("doc_type"),
            kyc.get("doc_number"),
            "APPROVED",
            "STANDARD",
            (datetime.now(timezone.utc) - timedelta(days=random.randint(30, 365))).isoformat(),
            None,  # last_login_at — populated below
            loc["country_code"],
            loc["city"],
            loc["lat"],
            loc["lon"],
            None,
            round(random.uniform(1000, 50000), 2),
            True,
            a.get("persona_type", "NormalTrader"),
            a.get("cluster_id"),
        ))

    execute_batch(cur, """
        INSERT INTO accounts (
            account_id, first_name, last_name, email, phone,
            nationality, date_of_birth, doc_type, doc_number,
            kyc_status, risk_tier, created_at,
            last_login_at, last_login_country, last_login_city,
            last_login_lat, last_login_lon, last_device_id,
            balance, is_active, persona_type, cluster_id
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (account_id) DO NOTHING
    """, account_rows, page_size=200)
    conn.commit()

    # ── Seed 30-day login history per account ─────────────────────────────
    login_rows = []
    now = datetime.now(timezone.utc)
    for a in accounts:
        loc = a["home_location"]
        device = generate_fingerprint(a["account_id"])
        num_logins = random.randint(5, 60)
        for _ in range(num_logins):
            days_ago = random.uniform(0, 30)
            login_time = now - timedelta(days=days_ago)
            login_rows.append((
                a["account_id"],
                login_time.isoformat(),
                loc["ip_address"],
                loc["country_code"],
                loc["city"],
                loc["lat"],
                loc["lon"],
                device["device_id"],
                True,
            ))

    execute_batch(cur, """
        INSERT INTO login_history (
            account_id, login_at, ip_address, country_code,
            city, lat, lon, device_id, success
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, login_rows, page_size=500)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("RDS seeding complete.")
