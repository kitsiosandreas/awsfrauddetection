"""
SystemAbuser persona — order/cancel hammering and API rate abuse.

Key signals:
- High order → cancel ratio (>80%)
- API call rate far exceeds normal limits
- Quote-stuffing pattern: burst of orders on same instrument
- Consistent throughout day (no business-hours pattern)
"""
import random
import uuid
from datetime import datetime, timezone

from .base import BaseTrader, INSTRUMENTS
from .normal_trader import _price
from utils.device_fingerprint import generate_fingerprint


API_ENDPOINTS = [
    "/v1/orders",
    "/v1/orders/{id}/cancel",
    "/v1/quotes",
    "/v1/positions",
    "/v1/account/balance",
    "/v1/instruments/{sym}/tick",
]


class SystemAbuser(BaseTrader):
    """
    Simulates a bot that hammers the trading API with order/cancel cycles
    to probe latency, exhaust quote streams, or test execution thresholds.
    """

    def __init__(self, account_id: str, kyc: dict, home_location: dict):
        super().__init__(account_id, kyc, home_location)
        self.burst_size = random.randint(20, 100)   # orders per cycle
        self.target_instrument = random.choice(INSTRUMENTS[:6])

    def generate_events(self) -> list[dict]:
        events = []
        device = generate_fingerprint(self.account_id, device_index=0)

        # Burst of API calls (quote stuffing)
        num_api = random.randint(self.burst_size, self.burst_size * 3)
        for _ in range(num_api):
            endpoint = random.choice(API_ENDPOINTS)
            events.append(self._api_call_event(
                endpoint=endpoint,
                method="POST" if "orders" in endpoint else "GET",
                response_code=random.choices([200, 429, 400], weights=[70, 20, 10])[0],
                label="api_abuse",
            ))

        # Place order → immediately cancel (order/cancel cycling)
        num_orders = random.randint(10, 40)
        for _ in range(num_orders):
            price = _price(self.target_instrument)
            # PLACE
            order_id = str(uuid.uuid4())
            place = self._trade_event(
                instrument=self.target_instrument,
                direction=random.choice(["BUY", "SELL"]),
                lots=0.01,
                open_price=price,
                trade_type="LIMIT",
                label="order_cancel_abuse",
            )
            place["order_id"] = order_id
            events.append(place)

            # CANCEL (immediately)
            cancel = self._api_call_event(
                endpoint=f"/v1/orders/{order_id}/cancel",
                method="DELETE",
                response_code=200,
                label="order_cancel_abuse",
            )
            cancel["cancelled_order_id"] = order_id
            events.append(cancel)

        return events
