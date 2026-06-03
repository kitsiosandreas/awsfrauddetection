"""
Device fingerprint generator.
Produces realistic browser/app fingerprints for session events.
"""
import random
import uuid
import hashlib

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "MT4-Platform/4.00 build 1360",  # MetaTrader — legitimate trading app
    "MT5-Platform/5.00 build 3960",
]

SCREEN_RESOLUTIONS = [
    "1920x1080", "2560x1440", "1366x768", "1440x900", "3840x2160",
    "375x812",   "390x844",   "414x896",
]

PLATFORMS = ["Win32", "MacIntel", "Linux x86_64", "iPhone", "Android"]
LANGUAGES = ["en-AU", "en-GB", "en-US", "de-DE", "ja-JP", "zh-HK", "ar-AE"]
TIMEZONES = ["UTC+10", "UTC+0", "UTC+1", "UTC+8", "UTC+9", "UTC+4", "UTC+2"]


def generate_fingerprint(account_id: str, device_index: int = 0) -> dict:
    """
    Generate a deterministic device fingerprint for a given account+device index.
    Same inputs always return the same fingerprint — simulates a known device.
    """
    seed = f"{account_id}-{device_index}"
    rng = random.Random(seed)

    ua = rng.choice(USER_AGENTS)
    is_mobile = "Mobile" in ua or "iPhone" in ua or "Android" in ua

    # Stable canvas hash (simulated)
    canvas_hash = hashlib.md5(seed.encode()).hexdigest()[:16]

    return {
        "device_id": str(uuid.UUID(hashlib.md5(f"dev-{seed}".encode()).hexdigest())),
        "user_agent": ua,
        "screen_resolution": rng.choice(SCREEN_RESOLUTIONS),
        "platform": "iPhone" if "iPhone" in ua else ("Android" if "Android" in ua else rng.choice(PLATFORMS[:3])),
        "language": rng.choice(LANGUAGES),
        "timezone_offset": rng.choice(TIMEZONES),
        "canvas_fingerprint": canvas_hash,
        "is_mobile": is_mobile,
        "plugins_hash": hashlib.md5(f"plugins-{seed}".encode()).hexdigest()[:8],
    }


def generate_new_device(account_id: str) -> dict:
    """Generate a completely random (never-seen) device — used in ATO simulation."""
    random_seed = str(uuid.uuid4())
    rng = random.Random(random_seed)

    ua = rng.choice(USER_AGENTS)
    return {
        "device_id": str(uuid.uuid4()),
        "user_agent": ua,
        "screen_resolution": rng.choice(SCREEN_RESOLUTIONS),
        "platform": rng.choice(PLATFORMS),
        "language": rng.choice(LANGUAGES),
        "timezone_offset": rng.choice(TIMEZONES),
        "canvas_fingerprint": uuid.uuid4().hex[:16],
        "is_mobile": "Mobile" in ua,
        "plugins_hash": uuid.uuid4().hex[:8],
        "is_new_device": True,
    }
