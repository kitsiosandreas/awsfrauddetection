"""
Base persona — shared logic for all trader types.
"""
import uuid
import random
import time
from datetime import datetime, timezone
from typing import Optional

INSTRUMENTS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD",
    "XAUUSD", "XAGUSD", "BTCUSD", "ETHUSD",
    "US30",   "SPX500", "NAS100", "UK100", "DE40",
    "CRUDE",  "NATGAS",
]

DIRECTIONS = ["BUY", "SELL"]


class BaseTrader:
    """
    Base class for all trader personas.

    Subclasses must implement `generate_events()` which returns a list of
    dicts that will be published to the appropriate Kafka topic.
    """

    def __init__(self, account_id: str, kyc: dict, home_location: dict):
        self.account_id    = account_id
        self.kyc           = kyc
        self.home_location = home_location
        self.current_location = home_location.copy()

        # Behavioural fingerprint (set once at creation)
        self.preferred_instruments = random.sample(INSTRUMENTS, k=random.randint(2, 5))
        self.peak_hour = random.randint(8, 18)   # preferred trading hour (local)
        self.avg_trade_size = random.uniform(0.1, 5.0)   # lots
        self.avg_hold_min   = random.uniform(5, 480)      # minutes
        self.session_device_index = 0

    # ── Event factories ───────────────────────────────────────────────────────

    def _trade_event(
        self,
        instrument: str,
        direction: str,
        lots: float,
        open_price: float,
        close_price: Optional[float] = None,
        trade_type: str = "MARKET",
        label: str = "normal",
    ) -> dict:
        now = datetime.now(timezone.utc)
        return {
            "event_type":   "TRADE",
            "event_id":     str(uuid.uuid4()),
            "account_id":   self.account_id,
            "timestamp":    now.isoformat(),
            "epoch_ms":     int(now.timestamp() * 1000),
            "instrument":   instrument,
            "direction":    direction,
            "lots":         round(lots, 2),
            "open_price":   round(open_price, 5),
            "close_price":  round(close_price, 5) if close_price else None,
            "trade_type":   trade_type,
            "country_code": self.current_location["country_code"],
            "ip_address":   self.current_location["ip_address"],
            "label":        label,
        }

    def _session_event(
        self,
        event_subtype: str,  # LOGIN | LOGOUT | PASSWORD_RESET | WITHDRAWAL_REQUEST
        device: dict,
        location: dict,
        label: str = "normal",
        extra: dict = None,
    ) -> dict:
        now = datetime.now(timezone.utc)
        ev = {
            "event_type":    "SESSION",
            "event_subtype": event_subtype,
            "event_id":      str(uuid.uuid4()),
            "account_id":    self.account_id,
            "timestamp":     now.isoformat(),
            "epoch_ms":      int(now.timestamp() * 1000),
            "device_id":     device["device_id"],
            "user_agent":    device["user_agent"],
            "ip_address":    location["ip_address"],
            "country_code":  location["country_code"],
            "city":          location["city"],
            "lat":           location["lat"],
            "lon":           location["lon"],
            "label":         label,
        }
        if extra:
            ev.update(extra)
        return ev

    def _registration_event(
        self,
        device: dict,
        location: dict,
        label: str = "normal",
        extra: dict = None,
    ) -> dict:
        now = datetime.now(timezone.utc)
        ev = {
            "event_type":   "REGISTRATION",
            "event_id":     str(uuid.uuid4()),
            "account_id":   self.account_id,
            "timestamp":    now.isoformat(),
            "epoch_ms":     int(now.timestamp() * 1000),
            "email":        self.kyc["email"],
            "nationality":  self.kyc["nationality"],
            "device_id":    device["device_id"],
            "ip_address":   location["ip_address"],
            "country_code": location["country_code"],
            "form_fill_time_sec": random.uniform(30, 600),
            "label":        label,
        }
        if extra:
            ev.update(extra)
        return ev

    def _api_call_event(
        self,
        endpoint: str,
        method: str = "POST",
        response_code: int = 200,
        label: str = "normal",
    ) -> dict:
        now = datetime.now(timezone.utc)
        return {
            "event_type":    "API_CALL",
            "event_id":      str(uuid.uuid4()),
            "account_id":    self.account_id,
            "timestamp":     now.isoformat(),
            "epoch_ms":      int(now.timestamp() * 1000),
            "endpoint":      endpoint,
            "method":        method,
            "response_code": response_code,
            "ip_address":    self.current_location["ip_address"],
            "country_code":  self.current_location["country_code"],
            "label":         label,
        }

    def generate_events(self) -> list[dict]:
        """Override in subclass. Return list of event dicts."""
        raise NotImplementedError
