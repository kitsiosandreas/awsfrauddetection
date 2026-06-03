"""
Rule-based detection for Coordinated Trading.
These rules detect individual-level signals — the ML layer catches the ring-level patterns.
"""
from typing import List


def check_velocity_same_direction(
    recent_trades: List[dict],
    instrument: str,
    direction: str,
    window_seconds: int = 60,
    threshold: int = 5,
    current_epoch_ms: float = 0,
) -> dict:
    """
    Fired when account opens >N trades on the same instrument in the same direction
    within a rolling time window.
    """
    window_ms = window_seconds * 1000
    matching = [
        t for t in recent_trades
        if t.get("instrument") == instrument
        and t.get("direction") == direction
        and (current_epoch_ms - t.get("epoch_ms", 0)) <= window_ms
    ]
    triggered = len(matching) >= threshold
    return {
        "triggered": triggered,
        "count": len(matching),
        "threshold": threshold,
        "signal": "VELOCITY_SAME_INSTRUMENT_DIRECTION" if triggered else None,
    }


def check_spread_abuse(
    entry_price: float,
    exit_price: float,
    direction: str,
    min_pips: float = 2.0,
    instrument: str = "EURUSD",
) -> dict:
    """
    Fired when account consistently opens and closes within the minimum spread,
    indicating they are exploiting latency arbitrage.
    """
    pip_size = 0.0001 if "JPY" not in instrument else 0.01
    pnl_pips = (exit_price - entry_price) / pip_size
    if direction == "SELL":
        pnl_pips = -pnl_pips

    triggered = 0 < pnl_pips < min_pips
    return {
        "triggered": triggered,
        "pnl_pips": round(pnl_pips, 2),
        "min_pips": min_pips,
        "signal": "SPREAD_ABUSE" if triggered else None,
    }


def check_coordinated_timing(
    cluster_member_epochs: List[float],
    current_epoch_ms: float,
    sync_threshold_ms: float = 500.0,
) -> dict:
    """
    Fired when multiple members of the same cluster open positions within
    sync_threshold_ms of each other.
    """
    if len(cluster_member_epochs) < 2:
        return {"triggered": False, "signal": None}

    max_spread_ms = max(cluster_member_epochs) - min(cluster_member_epochs)
    triggered = max_spread_ms <= sync_threshold_ms
    return {
        "triggered": triggered,
        "max_spread_ms": round(max_spread_ms, 1),
        "sync_threshold_ms": sync_threshold_ms,
        "member_count": len(cluster_member_epochs),
        "signal": "COORDINATED_TIMING" if triggered else None,
    }


def check_correlated_pnl(
    account_pnl_series: List[float],
    peer_pnl_series: List[float],
    correlation_threshold: float = 0.85,
) -> dict:
    """
    Checks Pearson correlation between this account's P&L and a peer's.
    High correlation is a key coordinated ring signal.
    """
    if len(account_pnl_series) < 5 or len(peer_pnl_series) < 5:
        return {"triggered": False, "correlation": None, "signal": None}

    n = min(len(account_pnl_series), len(peer_pnl_series))
    x = account_pnl_series[-n:]
    y = peer_pnl_series[-n:]

    mean_x = sum(x) / n
    mean_y = sum(y) / n
    num = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    den_x = sum((xi - mean_x) ** 2 for xi in x) ** 0.5
    den_y = sum((yi - mean_y) ** 2 for yi in y) ** 0.5

    if den_x == 0 or den_y == 0:
        return {"triggered": False, "correlation": 0, "signal": None}

    corr = round(num / (den_x * den_y), 4)
    triggered = corr >= correlation_threshold
    return {
        "triggered": triggered,
        "correlation": corr,
        "threshold": correlation_threshold,
        "signal": "CORRELATED_PNL" if triggered else None,
    }
