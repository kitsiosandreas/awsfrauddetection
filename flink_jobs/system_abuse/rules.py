"""
Rule-based detection for System Abuse & Abusive Registration.
"""
from typing import List


def check_api_rate(
    api_calls_in_window: int,
    window_seconds: int = 60,
    threshold_per_min: int = 200,
) -> dict:
    """Fired when API call rate from an account exceeds the threshold."""
    normalised_per_min = api_calls_in_window * (60 / window_seconds)
    triggered = normalised_per_min > threshold_per_min
    return {
        "triggered": triggered,
        "rate_per_min": round(normalised_per_min, 1),
        "threshold": threshold_per_min,
        "signal": "API_RATE_EXCEEDED" if triggered else None,
    }


def check_order_cancel_ratio(
    orders: int,
    cancels: int,
    threshold: float = 0.80,
) -> dict:
    """
    Fired when order/cancel ratio exceeds threshold (quote stuffing signal).
    """
    if orders == 0:
        return {"triggered": False, "ratio": 0, "signal": None}
    ratio = cancels / orders
    triggered = ratio >= threshold
    return {
        "triggered": triggered,
        "ratio": round(ratio, 4),
        "threshold": threshold,
        "signal": "HIGH_ORDER_CANCEL_RATIO" if triggered else None,
    }


def check_registration_burst(
    registrations_from_ip_prefix: int,
    window_hours: int = 24,
    threshold: int = 5,
) -> dict:
    """
    Fired when >N accounts are registered from the same /24 IP prefix
    within the window. Key signal for multi-accounting / bonus farming.
    """
    triggered = registrations_from_ip_prefix >= threshold
    return {
        "triggered": triggered,
        "count": registrations_from_ip_prefix,
        "threshold": threshold,
        "window_hours": window_hours,
        "signal": "REGISTRATION_BURST_FROM_IP" if triggered else None,
    }


def check_bonus_farming(
    deposit_epoch_ms: float,
    bonus_claim_epoch_ms: float,
    withdrawal_epoch_ms: float,
    max_days: int = 3,
) -> dict:
    """
    CEP: deposit → bonus claim → withdrawal within max_days.
    Classic bonus farming pattern.
    """
    ms_per_day = 86400 * 1000
    deposit_to_withdrawal_days = (withdrawal_epoch_ms - deposit_epoch_ms) / ms_per_day
    bonus_claimed = bonus_claim_epoch_ms > deposit_epoch_ms

    triggered = bonus_claimed and 0 < deposit_to_withdrawal_days <= max_days
    return {
        "triggered": triggered,
        "deposit_to_withdrawal_days": round(deposit_to_withdrawal_days, 2),
        "max_days": max_days,
        "signal": "BONUS_FARMING" if triggered else None,
    }


def check_shared_canvas_fingerprint(
    account_ids_sharing_canvas: List[str],
    threshold: int = 2,
) -> dict:
    """
    Fired when multiple accounts share the same browser canvas fingerprint.
    Strong signal for multi-accounting / abusive registration rings.
    """
    triggered = len(account_ids_sharing_canvas) >= threshold
    return {
        "triggered": triggered,
        "shared_account_count": len(account_ids_sharing_canvas),
        "threshold": threshold,
        "signal": "SHARED_CANVAS_FINGERPRINT" if triggered else None,
    }
