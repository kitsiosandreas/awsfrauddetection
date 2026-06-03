"""
KYC / synthetic identity generator.
Produces realistic account registration data using Faker.
"""
import random
import uuid
import hashlib
from datetime import date, timedelta
from faker import Faker

_fakers = {
    "en_AU": Faker("en_AU"),
    "en_GB": Faker("en_GB"),
    "en_US": Faker("en_US"),
    "de_DE": Faker("de_DE"),
    "ja_JP": Faker("ja_JP"),
}

DISPOSABLE_DOMAINS = [
    "mailnull.com", "trashmail.com", "guerrillamail.com",
    "tempmail.com", "throwam.com", "yopmail.com",
]
LEGIT_DOMAINS = [
    "gmail.com", "outlook.com", "hotmail.com", "yahoo.com",
    "protonmail.com", "icloud.com",
]

NATIONALITIES = ["AU", "GB", "US", "DE", "JP", "SG", "HK", "AE", "ZA", "BR"]


def generate_identity(
    account_id: str,
    use_disposable_email: bool = False,
    slight_variation_of: dict = None,
) -> dict:
    """
    Generate a KYC identity record.

    Args:
        account_id:            Unique account identifier.
        use_disposable_email:  Use a throwaway email domain (bot signal).
        slight_variation_of:   Clone an existing identity with small mutations
                               (abusive registration signal).
    """
    locale = random.choice(list(_fakers.keys()))
    fake = _fakers[locale]

    dob = fake.date_of_birth(minimum_age=18, maximum_age=70)

    if slight_variation_of:
        # Slight mutations — same person, different email / phone
        base = slight_variation_of
        first = base["first_name"]
        last  = base["last_name"]
        dob   = date.fromisoformat(base["date_of_birth"]) + timedelta(days=random.randint(-2, 2))
        email = _mutate_email(base["email"])
        phone = fake.phone_number()
    else:
        first = fake.first_name()
        last  = fake.last_name()
        domain = random.choice(DISPOSABLE_DOMAINS if use_disposable_email else LEGIT_DOMAINS)
        email = f"{first.lower()}.{last.lower()}{random.randint(1, 999)}@{domain}"
        phone = fake.phone_number()

    nationality = random.choice(NATIONALITIES)

    # Deterministic doc number for same account — makes de-duplication easy
    doc_number = hashlib.md5(f"{account_id}-doc".encode()).hexdigest()[:12].upper()

    return {
        "account_id":    account_id,
        "first_name":    first,
        "last_name":     last,
        "full_name":     f"{first} {last}",
        "date_of_birth": dob.isoformat(),
        "nationality":   nationality,
        "email":         email,
        "phone":         phone,
        "doc_type":      random.choice(["PASSPORT", "DRIVERS_LICENSE", "NATIONAL_ID"]),
        "doc_number":    doc_number,
        "address": {
            "street": fake.street_address(),
            "city":   fake.city(),
            "state":  getattr(fake, "state", lambda: "")(),
            "zip":    fake.postcode(),
            "country": nationality,
        },
        "registration_date": date.today().isoformat(),
        "kyc_status": "PENDING",
        "risk_tier": "STANDARD",
    }


def _mutate_email(original: str) -> str:
    """Slightly mutate an email address — common abusive registration pattern."""
    local, domain = original.split("@")
    mutations = [
        lambda s: s + str(random.randint(1, 99)),
        lambda s: s.replace(".", "_"),
        lambda s: s + ".trade",
        lambda s: s[:-1] if len(s) > 4 else s + "x",
    ]
    new_local = random.choice(mutations)(local)
    new_domain = random.choice(LEGIT_DOMAINS + DISPOSABLE_DOMAINS)
    return f"{new_local}@{new_domain}"
