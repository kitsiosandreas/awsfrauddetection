"""
ATOAttacker persona — Account Takeover simulation.

Attack flow:
  Phase 1 (optional): credential stuffing — burst of failed logins before success
  Phase 2: successful login from NEW device + NEW geo (impossible travel)
  Phase 3: password reset
  Phase 4: large withdrawal request to new bank account
  Phase 5: rapid position close (drain open positions)

Key signals that detection should catch:
- New device fingerprint never seen on this account
- Geo-velocity: login country B vs. last known login country A within minutes
- password_reset immediately followed by withdrawal
- Session duration shorter than baseline
"""
import random
from datetime import datetime, timezone

from .base import BaseTrader
from utils.device_fingerprint import generate_fingerprint, generate_new_device
from utils.geo_data import pick_location


class ATOAttacker(BaseTrader):
    """
    Simulates a legitimate account being taken over.
    The account has 30 days of normal history (seeded in RDS).
    The attack event produces a burst of anomalous session activity.
    """

    def __init__(self, account_id: str, kyc: dict, home_location: dict):
        super().__init__(account_id, kyc, home_location)
        # Attack comes from a different geography than the account's home
        attack_country = self._pick_attack_country(home_location["country_code"])
        self.attacker_location = pick_location(attack_country)
        self.attacker_device   = generate_new_device(account_id)

    @staticmethod
    def _pick_attack_country(home_country: str) -> str:
        """Pick a geo far from home — maximises impossible-travel signal."""
        geo_pairs = {
            "AU": "NG", "GB": "VN", "US": "RU", "DE": "NG",
            "SG": "RU", "JP": "NG", "HK": "RU", "AE": "VN",
        }
        return geo_pairs.get(home_country, "NG")  # default to Nigeria

    def generate_events(self) -> list[dict]:
        events = []
        normal_device  = generate_fingerprint(self.account_id, device_index=0)
        attacker_dev   = self.attacker_device
        attacker_loc   = self.attacker_location

        # ── Phase 1: Optional credential stuffing ──────────────────────────
        if random.random() < 0.70:
            num_failures = random.randint(10, 30)
            for _ in range(num_failures):
                fail = self._session_event(
                    "LOGIN_FAILED", attacker_dev, attacker_loc,
                    label="credential_stuffing",
                    extra={"failure_reason": "INVALID_PASSWORD"},
                )
                events.append(fail)

        # ── Phase 2: Successful login from attacker geo + new device ────────
        login = self._session_event(
            "LOGIN", attacker_dev, attacker_loc,
            label="ato_takeover",
            extra={
                "is_new_device": True,
                "is_new_country": True,
                "last_known_country": self.home_location["country_code"],
                "last_known_city":    self.home_location["city"],
            },
        )
        events.append(login)

        # ── Phase 3: Password reset ──────────────────────────────────────────
        pw_reset = self._session_event(
            "PASSWORD_RESET", attacker_dev, attacker_loc,
            label="ato_takeover",
        )
        events.append(pw_reset)

        # ── Phase 4: Change withdrawal bank account ──────────────────────────
        bank_change = self._api_call_event(
            "/v1/account/bank-details", "PUT", 200, label="ato_takeover",
        )
        events.append(bank_change)

        # ── Phase 5: Large withdrawal ────────────────────────────────────────
        withdrawal_amount = round(random.uniform(5000, 50000), 2)
        withdrawal = self._session_event(
            "WITHDRAWAL_REQUEST", attacker_dev, attacker_loc,
            label="ato_takeover",
            extra={
                "amount": withdrawal_amount,
                "currency": "USD",
                "is_new_bank_account": True,
                "minutes_since_login": round(random.uniform(1, 8), 1),
            },
        )
        events.append(withdrawal)

        # ── Phase 6: Close all open positions (drain account) ────────────────
        num_closes = random.randint(1, 5)
        for _ in range(num_closes):
            close = self._api_call_event(
                "/v1/positions/close-all", "POST", 200, label="ato_takeover",
            )
            events.append(close)

        return events
