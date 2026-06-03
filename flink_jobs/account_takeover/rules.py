"""
Rule-based detection functions for Account Takeover (ATO).
These are called from the Flink job or can be unit-tested independently.
"""
import math


# ── Rule 1: Geo-velocity ─────────────────────────────────────────────────────
def check_geo_velocity(
    lat1: float, lon1: float, t1_epoch_ms: float,
    lat2: float, lon2: float, t2_epoch_ms: float,
    threshold_kph: float = 900.0,
) -> dict:
    """
    Returns alert signal if travel between two locations is physically impossible.

    Args:
        lat1/lon1/t1: Last known login location + epoch time in ms
        lat2/lon2/t2: Current login location + epoch time in ms
        threshold_kph: Max plausible speed (default 900 kph — commercial aircraft)

    Returns:
        {"triggered": True/False, "velocity_kph": float, "signal": str}
    """
    delta_hours = abs(t2_epoch_ms - t1_epoch_ms) / (3600 * 1000)
    if delta_hours < 0.001:
        return {
            "triggered": True,
            "velocity_kph": 999999.0,
            "signal": "GEO_VELOCITY_SIMULTANEOUS",
        }

    distance_km = _haversine_km(lat1, lon1, lat2, lon2)
    velocity_kph = distance_km / delta_hours

    return {
        "triggered": velocity_kph > threshold_kph,
        "velocity_kph": round(velocity_kph, 1),
        "signal": "GEO_VELOCITY_IMPOSSIBLE" if velocity_kph > threshold_kph else None,
    }


def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ── Rule 2: New device + withdrawal ──────────────────────────────────────────
def check_new_device_withdrawal(
    is_new_device: bool,
    has_withdrawal_in_window: bool,
    window_hours: float = 24.0,
) -> dict:
    """Fired when a never-seen device immediately requests a withdrawal."""
    triggered = is_new_device and has_withdrawal_in_window
    return {
        "triggered": triggered,
        "signal": "NEW_DEVICE_WITHDRAWAL" if triggered else None,
        "window_hours": window_hours,
    }


# ── Rule 3: Password reset → withdrawal CEP ──────────────────────────────────
def check_pw_reset_withdrawal_chain(
    pw_reset_epoch_ms: float,
    withdrawal_epoch_ms: float,
    window_minutes: float = 60.0,
) -> dict:
    """
    Fired when password reset is immediately followed by a withdrawal request.
    Classic ATO pattern: attacker resets password to lock out real owner,
    then drains account.
    """
    delta_min = (withdrawal_epoch_ms - pw_reset_epoch_ms) / (60 * 1000)
    triggered = 0 < delta_min <= window_minutes
    return {
        "triggered": triggered,
        "minutes_between": round(delta_min, 1),
        "signal": "PW_RESET_WITHDRAWAL_CHAIN" if triggered else None,
    }


# ── Rule 4: Credential stuffing ──────────────────────────────────────────────
def check_credential_stuffing(
    failed_logins_in_window: int,
    window_minutes: float = 5.0,
    threshold: int = 10,
) -> dict:
    """Fired on burst of failed logins from same IP preceding a success."""
    triggered = failed_logins_in_window >= threshold
    return {
        "triggered": triggered,
        "failed_count": failed_logins_in_window,
        "window_minutes": window_minutes,
        "signal": "CREDENTIAL_STUFFING" if triggered else None,
    }


# Dummy classes for import compatibility with job.py
class GeoVelocityRule:
    @staticmethod
    def check(event, state):
        return check_geo_velocity(
            state.get("last_lat", 0), state.get("last_lon", 0),
            state.get("last_login_epoch_ms", 0),
            event.get("lat", 0), event.get("lon", 0),
            event.get("epoch_ms", 0),
        )


class NewDeviceWithdrawalRule:
    @staticmethod
    def check(event, state):
        return check_new_device_withdrawal(
            event.get("is_new_device", False),
            state.get("has_pending_withdrawal", False),
        )


class PasswordResetWithdrawalCEP:
    @staticmethod
    def check(pw_reset_ms, withdrawal_ms):
        return check_pw_reset_withdrawal_chain(pw_reset_ms, withdrawal_ms)


class CredentialStuffingRule:
    @staticmethod
    def check(failed_count, window_min=5.0, threshold=10):
        return check_credential_stuffing(failed_count, window_min, threshold)
