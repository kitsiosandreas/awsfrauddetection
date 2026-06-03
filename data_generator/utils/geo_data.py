"""
Geo and IP utilities for realistic event generation.
"""
import random
import math
from typing import Tuple

# (country_code, city, lat, lon, timezone)
LOCATIONS = [
    ("AU", "Sydney",        -33.87, 151.21, "Australia/Sydney"),
    ("AU", "Melbourne",     -37.81, 144.96, "Australia/Sydney"),
    ("GB", "London",         51.51,  -0.13, "Europe/London"),
    ("DE", "Frankfurt",      50.11,   8.68, "Europe/Berlin"),
    ("SG", "Singapore",       1.35, 103.82, "Asia/Singapore"),
    ("US", "New York",       40.71, -74.01, "America/New_York"),
    ("US", "Chicago",        41.88, -87.63, "America/Chicago"),
    ("JP", "Tokyo",          35.68, 139.69, "Asia/Tokyo"),
    ("HK", "Hong Kong",      22.33, 114.19, "Asia/Hong_Kong"),
    ("AE", "Dubai",          25.20,  55.27, "Asia/Dubai"),
    ("ZA", "Cape Town",     -33.93,  18.42, "Africa/Johannesburg"),
    ("BR", "Sao Paulo",     -23.55, -46.64, "America/Sao_Paulo"),
    ("NG", "Lagos",           6.52,   3.38, "Africa/Lagos"),   # high-risk geo
    ("VN", "Ho Chi Minh",   10.82, 106.63, "Asia/Ho_Chi_Minh"),  # high-risk geo
    ("RU", "Moscow",         55.75,  37.62, "Europe/Moscow"),    # high-risk geo
]

HIGH_RISK_COUNTRIES = {"NG", "VN", "RU", "CN", "IR", "KP"}

# Fictional IP blocks per location for realism
IP_BLOCKS = {
    "AU": ["203.12.x.x", "121.200.x.x"],
    "GB": ["81.149.x.x", "5.148.x.x"],
    "DE": ["85.214.x.x", "188.40.x.x"],
    "SG": ["103.10.x.x", "175.41.x.x"],
    "US": ["72.21.x.x",  "198.51.x.x"],
    "JP": ["126.x.x.x",  "220.109.x.x"],
    "HK": ["1.36.x.x",   "45.64.x.x"],
    "AE": ["185.43.x.x", "91.74.x.x"],
    "ZA": ["196.11.x.x", "105.0.x.x"],
    "BR": ["177.92.x.x", "189.63.x.x"],
    "NG": ["41.58.x.x",  "197.210.x.x"],
    "VN": ["113.160.x.x","14.161.x.x"],
    "RU": ["95.24.x.x",  "188.130.x.x"],
}


def pick_location(preferred_country: str = None) -> dict:
    """Pick a realistic location, optionally biased toward a country."""
    if preferred_country:
        candidates = [l for l in LOCATIONS if l[0] == preferred_country]
        if candidates:
            loc = random.choice(candidates)
        else:
            loc = random.choice(LOCATIONS)
    else:
        loc = random.choice(LOCATIONS)
    return {
        "country_code": loc[0],
        "city": loc[1],
        "lat": loc[2] + random.uniform(-0.5, 0.5),
        "lon": loc[3] + random.uniform(-0.5, 0.5),
        "timezone": loc[4],
        "ip_address": _generate_ip(loc[0]),
        "is_high_risk": loc[0] in HIGH_RISK_COUNTRIES,
    }


def _generate_ip(country_code: str) -> str:
    blocks = IP_BLOCKS.get(country_code, ["10.x.x.x"])
    template = random.choice(blocks)
    parts = template.split(".")
    return ".".join(
        str(random.randint(1, 254)) if p == "x" else p
        for p in parts
    )


def geo_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine distance between two coordinates in km."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def is_impossible_travel(
    lat1: float, lon1: float, t1_epoch: float,
    lat2: float, lon2: float, t2_epoch: float,
    max_speed_kph: float = 900.0,
) -> bool:
    """Return True if travel between two points in the given time is physically impossible."""
    delta_hours = abs(t2_epoch - t1_epoch) / 3600.0
    if delta_hours < 0.001:
        return True  # Same-second login from different country
    distance = geo_distance_km(lat1, lon1, lat2, lon2)
    required_speed = distance / delta_hours
    return required_speed > max_speed_kph
