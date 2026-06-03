"""
CoordinatedRingMember persona — simulates a member of a coordinated abuse ring.

Key signals:
- Multiple ring members open the SAME instrument in the SAME direction
  within a tight time window (50-500 ms apart)
- Shared IP subnet
- Correlated P&L — they always exit at approximately the same time
- No individual account breaches velocity rules; the pattern only shows
  up when looking across the whole cluster
"""
import random
import time
import threading
from datetime import datetime, timezone
from typing import ClassVar

from .base import BaseTrader, INSTRUMENTS, DIRECTIONS
from .normal_trader import _price
from utils.device_fingerprint import generate_fingerprint
from utils.geo_data import pick_location


class _RingCoordinator:
    """
    Singleton coordinator shared across all ring members in the same run.
    Manages: cluster assignment, shared IP block, and synchronised signal timing.
    """
    _lock: ClassVar[threading.Lock] = threading.Lock()
    _clusters: ClassVar[dict] = {}   # cluster_id → {instrument, direction, epoch_ms}

    @classmethod
    def get_or_create_cluster(cls, cluster_id: str) -> dict:
        with cls._lock:
            if cluster_id not in cls._clusters:
                cls._clusters[cluster_id] = {
                    "instrument": random.choice(["EURUSD", "GBPUSD", "XAUUSD", "US30"]),
                    "direction":  random.choice(DIRECTIONS),
                    "signal_epoch_ms": int(time.time() * 1000),
                    "ip_prefix":  f"192.168.{random.randint(10, 99)}",
                }
            return cls._clusters[cluster_id]

    @classmethod
    def refresh_signal(cls, cluster_id: str):
        """Called periodically to rotate the coordinated signal."""
        with cls._lock:
            if cluster_id in cls._clusters:
                cls._clusters[cluster_id]["signal_epoch_ms"] = int(time.time() * 1000)
                # Occasionally rotate instrument/direction
                if random.random() < 0.3:
                    cls._clusters[cluster_id]["instrument"] = random.choice(
                        ["EURUSD", "GBPUSD", "XAUUSD", "US30"]
                    )
                    cls._clusters[cluster_id]["direction"] = random.choice(DIRECTIONS)


class CoordinatedRingMember(BaseTrader):
    """
    Each ring member belongs to a cluster (cluster_id).
    Within a cluster, all members execute correlated trades to simulate
    coordinated abuse of the platform.
    """

    def __init__(self, account_id: str, kyc: dict, home_location: dict, cluster_id: str):
        super().__init__(account_id, kyc, home_location)
        self.cluster_id = cluster_id
        self.member_jitter_ms = random.randint(50, 500)  # sync offset

        # Override IP to share subnet with cluster
        cluster = _RingCoordinator.get_or_create_cluster(cluster_id)
        ip_suffix = random.randint(2, 254)
        self.current_location = home_location.copy()
        self.current_location["ip_address"] = f"{cluster['ip_prefix']}.{ip_suffix}"

    def generate_events(self) -> list[dict]:
        events = []
        cluster = _RingCoordinator.get_or_create_cluster(self.cluster_id)
        device = generate_fingerprint(self.account_id, device_index=0)

        # Each ring member fires a trade slightly after the cluster signal
        instrument = cluster["instrument"]
        direction  = cluster["direction"]
        base_price = _price(instrument)

        # Slight price variation to avoid exact match (harder to catch with simple rules)
        price_noise = base_price * random.uniform(0.0001, 0.0005)
        open_p = base_price + price_noise

        events.append(self._trade_event(
            instrument=instrument,
            direction=direction,
            lots=round(random.uniform(0.5, 2.0), 2),
            open_price=open_p,
            close_price=None,
            label="coordinated_ring",
            trade_type="MARKET",
        ))

        # Enrich with cluster metadata for downstream detection
        events[-1].update({
            "cluster_id": self.cluster_id,
            "cluster_jitter_ms": self.member_jitter_ms,
            "correlated_ip_prefix": cluster["ip_prefix"],
        })

        # 70% chance of exit trade (same direction close — take profit at predictable pip target)
        if random.random() < 0.70:
            pip_value = base_price * 0.0010
            profit_pips = random.uniform(3, 12)
            close_p = open_p + (pip_value * profit_pips if direction == "BUY" else -pip_value * profit_pips)
            exit_event = self._trade_event(
                instrument=instrument,
                direction="SELL" if direction == "BUY" else "BUY",
                lots=events[0]["lots"],
                open_price=close_p,
                close_price=close_p,
                label="coordinated_ring_exit",
            )
            exit_event.update({"cluster_id": self.cluster_id})
            events.append(exit_event)

        # Refresh cluster signal 30% of the time
        if random.random() < 0.30:
            _RingCoordinator.refresh_signal(self.cluster_id)

        return events
