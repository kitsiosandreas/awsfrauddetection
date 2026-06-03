"""
Fraud Detection Demo — Synthetic Data Generator
================================================
Entry point. Builds the account population, seeds RDS + S3,
then continuously emits events to MSK at the configured TPS.

Usage (local):
    python main.py --scenario mixed --tps 100 --accounts 500

Usage (ECS / env-var driven):
    All flags map to environment variables — see config.py.
"""
import argparse
import logging
import os
import random
import signal
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from config import Config
from personas.normal_trader import NormalTrader
from personas.coordinated_ring import CoordinatedRingMember
from personas.ato_attacker import ATOAttacker
from personas.system_abuser import SystemAbuser
from personas.abusive_registrant import AbusiveRegistrant
from producers.kafka_producer import build_producer, send_event
from seeders.rds_seeder import seed_database
from seeders.s3_seeder import upload_enrichment_data
from utils.kyc_generator import generate_identity
from utils.geo_data import pick_location

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("generator")

PERSONA_CLASS_MAP = {
    "NormalTrader":        NormalTrader,
    "CoordinatedRingMember": CoordinatedRingMember,
    "ATOAttacker":         ATOAttacker,
    "SystemAbuser":        SystemAbuser,
    "AbusiveRegistrant":   AbusiveRegistrant,
}


# ── Topic routing ─────────────────────────────────────────────────────────────
def _route_event(event: dict) -> str:
    etype = event.get("event_type", "")
    if etype == "TRADE":
        return Config.TOPIC_TRADES
    elif etype == "SESSION":
        return Config.TOPIC_SESSIONS
    elif etype == "REGISTRATION":
        return Config.TOPIC_REGISTRATIONS
    elif etype == "API_CALL":
        return Config.TOPIC_API_CALLS
    return Config.TOPIC_TRADES  # fallback


# ── Account population factory ────────────────────────────────────────────────
def build_population(total: int, ratios: dict) -> list[dict]:
    """Build list of account dicts with assigned personas."""
    logger.info(f"Building population of {total} accounts (ratios: {ratios})")
    accounts = []

    # Pre-compute cluster IDs for coordinated ring members
    ring_clusters: dict[int, str] = {}

    for i in range(total):
        # Pick persona proportionally
        persona_type = random.choices(
            list(ratios.keys()),
            weights=list(ratios.values()),
        )[0]

        account_id   = str(uuid.uuid4())
        location     = pick_location()
        kyc          = generate_identity(
            account_id,
            use_disposable_email=(persona_type == "AbusiveRegistrant"),
        )

        entry = {
            "account_id":   account_id,
            "kyc":          kyc,
            "home_location": location,
            "persona_type": persona_type,
        }

        # Assign ring members to clusters of 3-10
        if persona_type == "CoordinatedRingMember":
            cluster_slot = i % max(1, int(total * ratios.get("CoordinatedRingMember", 0.1) / 6))
            if cluster_slot not in ring_clusters:
                ring_clusters[cluster_slot] = str(uuid.uuid4())
            entry["cluster_id"] = ring_clusters[cluster_slot]

        # Assign abusive registrants to groups of 3-8
        if persona_type == "AbusiveRegistrant":
            group_slot = i % max(1, int(total * ratios.get("AbusiveRegistrant", 0.06) / 4))
            entry["group_id"] = f"group-{group_slot:04d}"

        accounts.append(entry)

    return accounts


def _build_persona(entry: dict):
    """Instantiate the correct persona class from an account entry."""
    ptype = entry["persona_type"]
    aid   = entry["account_id"]
    kyc   = entry["kyc"]
    loc   = entry["home_location"]

    if ptype == "CoordinatedRingMember":
        return CoordinatedRingMember(aid, kyc, loc, entry.get("cluster_id", str(uuid.uuid4())))
    elif ptype == "AbusiveRegistrant":
        return AbusiveRegistrant(aid, kyc, loc, entry.get("group_id", "g-0"))
    elif ptype in PERSONA_CLASS_MAP:
        return PERSONA_CLASS_MAP[ptype](aid, kyc, loc)
    else:
        return NormalTrader(aid, kyc, loc)


# ── Main generation loop ──────────────────────────────────────────────────────
def run_generator(
    producer,
    personas: list,
    tps: int,
    stop_flag: dict,
) -> None:
    """Continuous event emission loop."""
    target_interval = 1.0 / tps  # seconds between events
    sent_total = 0
    t0 = time.monotonic()
    report_interval = 10  # seconds

    logger.info(f"Starting event loop — target {tps} TPS across {len(personas)} personas")

    while not stop_flag["stop"]:
        loop_start = time.monotonic()
        batch_events = []

        # Each persona contributes events at its own rate
        active = random.sample(personas, min(tps, len(personas)))
        for persona in active:
            try:
                events = persona.generate_events()
                for ev in events:
                    topic = _route_event(ev)
                    batch_events.append((topic, ev, ev.get("account_id")))
            except Exception as exc:
                logger.warning(f"Persona {persona.account_id} error: {exc}")

        # Send batch
        for topic, event, key in batch_events:
            send_event(producer, topic, event, key=key)
            sent_total += 1

        producer.flush()

        # Rate control
        elapsed = time.monotonic() - loop_start
        sleep_time = max(0, target_interval - elapsed)
        time.sleep(sleep_time)

        # Progress report
        if (time.monotonic() - t0) > report_interval:
            actual_tps = sent_total / max(1, time.monotonic() - t0)
            logger.info(f"Sent {sent_total} events | actual TPS: {actual_tps:.1f}")
            t0 = time.monotonic()
            sent_total = 0


# ── CLI entry point ───────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Fraud Detection Data Generator")
    parser.add_argument("--scenario",  default=None, help="Override SCENARIO env var")
    parser.add_argument("--tps",       type=int, default=None, help="Events per second")
    parser.add_argument("--accounts",  type=int, default=None, help="Total accounts")
    parser.add_argument("--no-seed",   action="store_true", help="Skip RDS/S3 seeding")
    args = parser.parse_args()

    # Allow CLI args to override environment
    if args.scenario:  Config.SCENARIO = args.scenario
    if args.tps:       Config.TPS = args.tps
    if args.accounts:  Config.TOTAL_ACCOUNTS = args.accounts

    logger.info(
        f"Config: scenario={Config.SCENARIO}, tps={Config.TPS}, "
        f"accounts={Config.TOTAL_ACCOUNTS}, fraud_rate={Config.FRAUD_RATE}"
    )

    # ── Seed phase ────────────────────────────────────────────────────────
    if not args.no_seed:
        try:
            ratios = Config.persona_ratios()
            accounts = build_population(Config.TOTAL_ACCOUNTS, ratios)

            upload_enrichment_data(Config.ENRICHMENT_BUCKET, Config.AWS_REGION)
            seed_database(accounts)
        except Exception as exc:
            logger.error(f"Seeding failed (continuing without seed): {exc}")
            accounts = build_population(Config.TOTAL_ACCOUNTS, Config.persona_ratios())
    else:
        accounts = build_population(Config.TOTAL_ACCOUNTS, Config.persona_ratios())

    personas = [_build_persona(a) for a in accounts]
    logger.info(
        f"Personas built: "
        + ", ".join(f"{k}={sum(1 for a in accounts if a['persona_type']==k)}"
                    for k in set(a["persona_type"] for a in accounts))
    )

    # ── Kafka producer ────────────────────────────────────────────────────
    producer = build_producer(
        bootstrap_servers=Config.MSK_BOOTSTRAP_SERVERS,
        use_iam_auth=Config.USE_IAM_AUTH,
        region=Config.AWS_REGION,
        msk_cluster_arn=Config.MSK_CLUSTER_ARN,
    )

    # ── Graceful shutdown ─────────────────────────────────────────────────
    stop_flag = {"stop": False}

    def _handle_signal(sig, frame):
        logger.info("Shutdown signal received — flushing and exiting.")
        stop_flag["stop"] = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # ── Run ───────────────────────────────────────────────────────────────
    try:
        run_generator(producer, personas, Config.TPS, stop_flag)
    finally:
        producer.flush()
        producer.close()
        logger.info("Generator stopped.")


if __name__ == "__main__":
    main()
