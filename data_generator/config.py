"""
Generator configuration — all values resolved from environment variables
with sensible defaults for local development.
"""
import os


class Config:
    # ── Scenario control ──────────────────────────────────────────────────────
    SCENARIO: str = os.getenv("SCENARIO", "mixed")
    # mixed | normal | coordinated_ring | ato_attack | registration_burst | system_abuse

    TPS: int = int(os.getenv("TPS", "100"))
    TOTAL_ACCOUNTS: int = int(os.getenv("TOTAL_ACCOUNTS", "500"))
    FRAUD_RATE: float = float(os.getenv("FRAUD_RATE", "0.15"))

    # ── MSK ───────────────────────────────────────────────────────────────────
    MSK_CLUSTER_ARN: str = os.getenv("MSK_CLUSTER_ARN", "")
    MSK_BOOTSTRAP_SERVERS: str = os.getenv("MSK_BOOTSTRAP_SERVERS", "localhost:9092")
    # If running locally without IAM: set USE_IAM_AUTH=false
    USE_IAM_AUTH: bool = os.getenv("USE_IAM_AUTH", "true").lower() == "true"

    # ── RDS ───────────────────────────────────────────────────────────────────
    RDS_ENDPOINT: str = os.getenv("RDS_ENDPOINT", "localhost")
    RDS_PORT: int = int(os.getenv("RDS_PORT", "5432"))
    RDS_DB: str = os.getenv("RDS_DB", "fraud_detection")
    RDS_SECRET_ARN: str = os.getenv("RDS_SECRET_ARN", "")
    # For local dev only — not used when RDS_SECRET_ARN is set
    RDS_USER: str = os.getenv("RDS_USER", "fraud_admin")
    RDS_PASSWORD: str = os.getenv("RDS_PASSWORD", "localdevpassword")

    # ── S3 ────────────────────────────────────────────────────────────────────
    RAW_DATA_BUCKET: str = os.getenv("RAW_DATA_BUCKET", "")
    ENRICHMENT_BUCKET: str = os.getenv("ENRICHMENT_BUCKET", "")
    AWS_REGION: str = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    # ── Kafka topics ─────────────────────────────────────────────────────────
    TOPIC_TRADES: str = "trades.raw"
    TOPIC_SESSIONS: str = "sessions.raw"
    TOPIC_REGISTRATIONS: str = "registrations.raw"
    TOPIC_API_CALLS: str = "api.calls"

    # ── Scenario ratios (% of accounts assigned per persona type) ────────────
    PERSONA_RATIOS = {
        "normal":              {"NormalTrader": 1.0},
        "coordinated_ring":    {"NormalTrader": 0.70, "CoordinatedRingMember": 0.30},
        "ato_attack":          {"NormalTrader": 0.85, "ATOAttacker": 0.15},
        "registration_burst":  {"NormalTrader": 0.60, "AbusiveRegistrant": 0.25, "SystemAbuser": 0.15},
        "system_abuse":        {"NormalTrader": 0.70, "SystemAbuser": 0.30},
        "mixed": {
            "NormalTrader":          0.75,
            "CoordinatedRingMember": 0.08,
            "ATOAttacker":           0.07,
            "AbusiveRegistrant":     0.06,
            "SystemAbuser":          0.04,
        },
    }

    @classmethod
    def persona_ratios(cls) -> dict:
        return cls.PERSONA_RATIOS.get(cls.SCENARIO, cls.PERSONA_RATIOS["mixed"])
