"""
AbusiveRegistrant persona — multi-accounting and bonus farming.

Key signals:
- Multiple accounts from same /24 IP block
- Similar KYC fields with slight mutations
- Deposit minimum → claim bonus → withdraw immediately
- Short account lifetime
- Disposable email domains
"""
import random
import uuid
from datetime import datetime, timezone

from .base import BaseTrader
from utils.kyc_generator import generate_identity, _mutate_email
from utils.device_fingerprint import generate_fingerprint, generate_new_device
from utils.geo_data import pick_location


class AbusiveRegistrant(BaseTrader):
    """
    Simulates registration abuse: the same human creating multiple accounts
    to farm sign-up bonuses or exploit new-account promotions.
    """

    # Shared state: group of related registrants share an IP prefix
    _registrant_groups: dict = {}

    def __init__(
        self,
        account_id: str,
        kyc: dict,
        home_location: dict,
        group_id: str,
        template_kyc: dict = None,
    ):
        super().__init__(account_id, kyc, home_location)
        self.group_id = group_id
        self.template_kyc = template_kyc  # Base identity to mutate from

        if group_id not in self._registrant_groups:
            self._registrant_groups[group_id] = {
                "ip_prefix": f"10.{random.randint(1,254)}.{random.randint(1,254)}",
                "device_canvas": uuid.uuid4().hex[:16],  # shared device signal
            }
        group = self._registrant_groups[group_id]
        # Override location to shared IP subnet
        self.current_location = home_location.copy()
        self.current_location["ip_address"] = (
            f"{group['ip_prefix']}.{random.randint(2, 254)}"
        )

    def generate_events(self) -> list[dict]:
        events = []
        group = self._registrant_groups[self.group_id]

        # Shared canvas fingerprint — key detection signal
        device = generate_fingerprint(self.account_id, 0)
        device["canvas_fingerprint"] = group["device_canvas"]  # Same canvas → same person

        location = self.current_location

        # Registration event
        reg = self._registration_event(
            device=device,
            location=location,
            label="abusive_registration",
            extra={
                "group_id": self.group_id,
                "bonus_eligible": True,
                "use_disposable_email": "@" + self.kyc["email"].split("@")[1]
                    in ["mailnull.com", "trashmail.com", "guerrillamail.com",
                        "tempmail.com", "throwam.com", "yopmail.com"],
            },
        )
        events.append(reg)

        # Minimal deposit → bonus claim → withdrawal
        if random.random() < 0.85:
            events.append(self._session_event(
                "LOGIN", device, location, label="abusive_registration",
            ))
            # Deposit (minimum for bonus)
            events.append(self._api_call_event(
                "/v1/account/deposit", "POST", 200, label="bonus_farming",
            ))
            # Claim bonus
            events.append(self._api_call_event(
                "/v1/promotions/claim", "POST", 200, label="bonus_farming",
            ))
            # Immediate withdrawal
            events.append(self._session_event(
                "WITHDRAWAL_REQUEST", device, location,
                label="bonus_farming",
                extra={
                    "amount": round(random.uniform(50, 300), 2),
                    "currency": "USD",
                    "days_since_deposit": random.randint(0, 2),
                    "group_id": self.group_id,
                },
            ))

        return events
