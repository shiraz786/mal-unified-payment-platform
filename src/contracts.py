from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List

MANDATORY_V2 = [
    "event_id", "contract_version", "payment_type", "customer_id", "amount", "currency",
    "event_timestamp", "status", "payment_method", "raw_source", "ingestion_ts"
]
ALLOWED_STATUS = {"PENDING", "SUCCESS", "FAILED", "REVERSED"}
ALLOWED_PAYMENT_TYPES = {"CARD", "TRANSFER", "BILL_PAYMENT"}


@dataclass
class ValidationError:
    source: str
    row_number: int
    reason: str
    record: Dict


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def migrate_v1_to_v2(record: Dict) -> Dict:
    """Example breaking-change migration: add country_code and is_international in v2."""
    migrated = dict(record)
    migrated["contract_version"] = "v2"
    migrated.setdefault("country_code", "AE")
    migrated.setdefault("is_international", False)
    migrated.setdefault("channel", "UNKNOWN")
    return migrated


def validate_record(record: Dict, row_number: int, source: str) -> List[ValidationError]:
    errors: List[ValidationError] = []
    for col in MANDATORY_V2:
        if record.get(col) in (None, ""):
            errors.append(ValidationError(source, row_number, f"missing mandatory field: {col}", record))

    if record.get("payment_type") not in ALLOWED_PAYMENT_TYPES:
        errors.append(ValidationError(source, row_number, "invalid payment_type", record))
    if record.get("status") not in ALLOWED_STATUS:
        errors.append(ValidationError(source, row_number, "invalid status", record))

    try:
        amount = float(record.get("amount", 0))
        if amount <= 0:
            errors.append(ValidationError(source, row_number, "amount must be > 0", record))
    except Exception:
        errors.append(ValidationError(source, row_number, "amount is not numeric", record))

    currency = str(record.get("currency", ""))
    if len(currency) != 3 or not currency.isalpha() or currency.upper() != currency:
        errors.append(ValidationError(source, row_number, "currency must be ISO-4217 style like AED", record))

    try:
        datetime.fromisoformat(str(record.get("event_timestamp")).replace("Z", "+00:00"))
    except Exception:
        errors.append(ValidationError(source, row_number, "event_timestamp must be ISO-8601", record))

    return errors
