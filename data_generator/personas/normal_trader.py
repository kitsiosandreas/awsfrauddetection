"""
NormalTrader persona — realistic benign trading behaviour.
"""
import random
from datetime import datetime, timezone

from .base import BaseTrader, INSTRUMENTS, DIRECTIONS
from utils.device_fingerprint import generate_fingerprint
from utils.geo_data import pick_location

# Realistic mid-prices (approximate)
BASE_PRICES = {
    "EURUSD": 1.0820, "GBPUSD": 1.2650, "USDJPY": 148.50,
    "AUDUSD": 0.6540, "USDCAD": 1.3490, "XAUUSD": 2020.0,
    "XAGUSD": 22.50,  "BTCUSD": 42000.0, "ETHUSD": 2250.0,
    "US30": 38500.0,  "SPX500": 4900.0,  "NAS100": 17400.0,
    "UK100": 7650.0,  "DE40": 16900.0,   "CRUDE": 73.50,
    "NATGAS": 1.85,
}


def _price(instrument: str) -> float:
    base = BASE_PRICES.get(instrument, 1.0)
    return base * random.uniform(0.998, 1.002)


class NormalTrader(BaseTrader):
    """
    Simulates a typical retail Forex/CFD trader:
    - 1-5 trades per cycle during business hours
    - Trades a small set of preferred instruments
    - Logs in from known device + location
    - Occasional session activity (check account, etc.)
    """

    def generate_events(self) -> list[dict]:
        events = []
        now = datetime.now(timezone.utc)

        # Business-hours bias (lower activity outside peak)
        if now.hour not in range(6, 21):
            if random.random() > 0.1:
                return []

        device = generate_fingerprint(self.account_id, device_index=0)
        location = self.home_location

        # ~30% chance of a login event per cycle
        if random.random() < 0.30:
            events.append(self._session_event("LOGIN", device, location))

        # 1-3 trades per active cycle
        num_trades = random.randint(1, 3)
        for _ in range(num_trades):
            instr = random.choice(self.preferred_instruments)
            direction = random.choice(DIRECTIONS)
            open_p = _price(instr)
            spread = open_p * 0.0002
            close_p = open_p + (spread if direction == "BUY" else -spread) * random.uniform(-5, 5)
            events.append(self._trade_event(
                instrument=instr,
                direction=direction,
                lots=round(self.avg_trade_size * random.uniform(0.5, 1.5), 2),
                open_price=open_p,
                close_price=close_p,
                label="normal",
            ))

        # ~5% chance of a withdrawal request (benign)
        if random.random() < 0.05:
            events.append(self._session_event(
                "WITHDRAWAL_REQUEST", device, location,
                extra={"amount": round(random.uniform(100, 5000), 2), "currency": "USD"},
            ))

        return events
