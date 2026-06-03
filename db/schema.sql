-- ============================================================
-- Fraud Detection Demo — PostgreSQL Schema
-- Run by rds_seeder.py on first startup
-- ============================================================

-- Enable pgcrypto for UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── Accounts ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS accounts (
    account_id            VARCHAR(36)      PRIMARY KEY,
    first_name            VARCHAR(100)     NOT NULL,
    last_name             VARCHAR(100)     NOT NULL,
    email                 VARCHAR(255)     NOT NULL UNIQUE,
    phone                 VARCHAR(50),
    nationality           CHAR(2),
    date_of_birth         DATE,
    doc_type              VARCHAR(30),
    doc_number            VARCHAR(50),
    kyc_status            VARCHAR(20)      NOT NULL DEFAULT 'PENDING',
                                           -- PENDING | APPROVED | REJECTED | SUSPENDED
    risk_tier             VARCHAR(20)      NOT NULL DEFAULT 'STANDARD',
                                           -- STANDARD | ELEVATED | HIGH
    created_at            TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    last_login_at         TIMESTAMPTZ,
    last_login_country    CHAR(2),
    last_login_city       VARCHAR(100),
    last_login_lat        DOUBLE PRECISION,
    last_login_lon        DOUBLE PRECISION,
    last_device_id        VARCHAR(36),
    balance               NUMERIC(18,2)    NOT NULL DEFAULT 0,
    is_active             BOOLEAN          NOT NULL DEFAULT TRUE,
    is_suspended          BOOLEAN          NOT NULL DEFAULT FALSE,
    suspension_reason     TEXT,
    persona_type          VARCHAR(50),     -- for demo only
    cluster_id            VARCHAR(36),     -- coordinated ring cluster
    group_id              VARCHAR(36)      -- abusive registration group
);

-- ── Login history ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS login_history (
    id                BIGSERIAL        PRIMARY KEY,
    account_id        VARCHAR(36)      NOT NULL REFERENCES accounts(account_id),
    login_at          TIMESTAMPTZ      NOT NULL,
    ip_address        VARCHAR(45),
    country_code      CHAR(2),
    city              VARCHAR(100),
    lat               DOUBLE PRECISION,
    lon               DOUBLE PRECISION,
    device_id         VARCHAR(36),
    user_agent        TEXT,
    success           BOOLEAN          NOT NULL DEFAULT TRUE,
    failure_reason    VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_login_account_time
    ON login_history(account_id, login_at DESC);

-- ── Session events ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS session_events (
    id                BIGSERIAL        PRIMARY KEY,
    event_id          VARCHAR(36)      NOT NULL UNIQUE,
    account_id        VARCHAR(36)      NOT NULL REFERENCES accounts(account_id),
    event_subtype     VARCHAR(50)      NOT NULL,
                                       -- LOGIN | LOGOUT | PASSWORD_RESET | WITHDRAWAL_REQUEST
    occurred_at       TIMESTAMPTZ      NOT NULL,
    ip_address        VARCHAR(45),
    country_code      CHAR(2),
    device_id         VARCHAR(36),
    label             VARCHAR(50)      DEFAULT 'normal'
);

-- ── Trade history ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trade_history (
    id                BIGSERIAL        PRIMARY KEY,
    trade_id          VARCHAR(36)      NOT NULL UNIQUE,
    account_id        VARCHAR(36)      NOT NULL REFERENCES accounts(account_id),
    traded_at         TIMESTAMPTZ      NOT NULL,
    instrument        VARCHAR(20)      NOT NULL,
    direction         CHAR(4)          NOT NULL,   -- BUY | SELL
    lots              NUMERIC(10,2)    NOT NULL,
    open_price        NUMERIC(18,5),
    close_price       NUMERIC(18,5),
    pnl               NUMERIC(18,2),
    hold_duration_min NUMERIC(10,2),
    country_code      CHAR(2),
    ip_address        VARCHAR(45),
    cluster_id        VARCHAR(36),
    label             VARCHAR(50)      DEFAULT 'normal'
);

CREATE INDEX IF NOT EXISTS idx_trade_account_time
    ON trade_history(account_id, traded_at DESC);
CREATE INDEX IF NOT EXISTS idx_trade_cluster
    ON trade_history(cluster_id, traded_at DESC) WHERE cluster_id IS NOT NULL;

-- ── API calls ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS api_calls (
    id                BIGSERIAL        PRIMARY KEY,
    call_id           VARCHAR(36)      NOT NULL UNIQUE,
    account_id        VARCHAR(36)      NOT NULL REFERENCES accounts(account_id),
    called_at         TIMESTAMPTZ      NOT NULL,
    endpoint          VARCHAR(200)     NOT NULL,
    method            VARCHAR(10)      NOT NULL,
    response_code     SMALLINT,
    ip_address        VARCHAR(45),
    label             VARCHAR(50)      DEFAULT 'normal'
);

CREATE INDEX IF NOT EXISTS idx_api_calls_account_time
    ON api_calls(account_id, called_at DESC);
CREATE INDEX IF NOT EXISTS idx_api_calls_ip
    ON api_calls(ip_address, called_at DESC);

-- ── IP reputation reference ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ip_reputation (
    ip_prefix         VARCHAR(20)      PRIMARY KEY,
    risk_score        NUMERIC(4,2)     NOT NULL DEFAULT 0,
    country_code      CHAR(2),
    is_vpn            BOOLEAN          NOT NULL DEFAULT FALSE,
    is_tor            BOOLEAN          NOT NULL DEFAULT FALSE,
    last_updated      TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

-- ── Alerts ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id          VARCHAR(36)      PRIMARY KEY,
    account_id        VARCHAR(36)      NOT NULL REFERENCES accounts(account_id),
    typology          VARCHAR(50)      NOT NULL,
    severity          VARCHAR(20)      NOT NULL,
    risk_score        NUMERIC(5,4),
    detection_method  VARCHAR(20),     -- RULE | ML | COMBINED
    signals           JSONB,
    cluster_id        VARCHAR(36),
    created_at        TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    status            VARCHAR(20)      NOT NULL DEFAULT 'NEW',
                                       -- NEW | REVIEWING | CLOSED_TP | CLOSED_FP
    analyst_note      TEXT
);

CREATE INDEX IF NOT EXISTS idx_alerts_account ON fraud_alerts(account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_typology ON fraud_alerts(typology, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON fraud_alerts(severity, created_at DESC);
