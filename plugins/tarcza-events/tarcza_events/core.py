# SPDX-License-Identifier: AGPL-3.0-or-later
"""Pure logic (no Django): envelope, Standard Webhooks signing, backoff, money, dedup keys.

Contract: tarcza-business-hub docs/specs/00-overview-and-contracts.md section 3.2/3.3 (v1.1).
"""
import base64
import hashlib
import hmac
import json
import secrets
import time
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

BACKOFF_BASE_SECONDS = 30
BACKOFF_CAP_SECONDS = 6 * 3600
GIVE_UP_AFTER_SECONDS = 72 * 3600


def new_ulid(ts_ms: int | None = None) -> str:
    ts = int(time.time() * 1000) if ts_ms is None else ts_ms
    n = (ts << 80) | int.from_bytes(secrets.token_bytes(10), "big")
    return "".join(_CROCKFORD[(n >> (5 * i)) & 31] for i in reversed(range(26)))


def new_msg_id() -> str:
    return "msg_" + new_ulid()


def _key(secret: str) -> bytes:
    return base64.b64decode(secret[6:] if secret.startswith("whsec_") else secret)


def sign(secret: str, msg_id: str, timestamp: int, body: bytes) -> str:
    mac = hmac.new(_key(secret), f"{msg_id}.{timestamp}.".encode() + body, hashlib.sha256).digest()
    return "v1," + base64.b64encode(mac).decode()


def delivery_headers(secret: str, msg_id: str, body: bytes, timestamp: int | None = None) -> dict:
    ts = int(time.time()) if timestamp is None else timestamp
    return {
        "webhook-id": msg_id,
        "webhook-timestamp": str(ts),
        "webhook-signature": sign(secret, msg_id, ts, body),
        "Content-Type": "application/json",
    }


def iso_utc(dt: datetime | None = None) -> str:
    dt = dt or datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def body_bytes(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":"), sort_keys=True).encode()


def envelope(event_type: str, tenant: str, subject: dict, data: dict, occurred_at: datetime | None = None) -> dict:
    return {
        "type": event_type,
        "version": 1,
        "tenant": tenant,
        "occurred_at": iso_utc(occurred_at),
        "source": "stock",
        "subject": subject,
        "data": data,
    }


def minor_units(amount, currency: str = "ZAR") -> int:
    """Integer minor units (2 decimals for ZAR/USD/EUR); never floats."""
    amount = getattr(amount, "amount", amount)  # djmoney Money -> Decimal
    return int((Decimal(str(amount)) * 100).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def money(amount, currency) -> dict | None:
    if amount is None or not currency:
        return None
    amount = getattr(amount, "amount", amount)
    return {"amount_minor": minor_units(amount, currency), "currency": str(currency)}


def dec_str(value) -> str:
    """Decimal as plain string without exponent or trailing zeros noise."""
    d = Decimal(str(value))
    s = format(d.normalize(), "f")
    return "0" if s in ("-0", "") else s


def next_delay(attempts: int) -> int:
    """Seconds to wait after the nth failed attempt (1-based): 30s doubling, capped at 6h."""
    return min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * 2 ** max(0, attempts - 1))


def is_dead(created_at: datetime, now: datetime) -> bool:
    return (now - created_at).total_seconds() > GIVE_UP_AFTER_SECONDS


def received_key(order_id: int, item_ids) -> str:
    h = hashlib.sha256(f"{order_id}:{','.join(str(i) for i in sorted(int(i) for i in item_ids))}".encode()).hexdigest()[:24]
    return f"stock.received:{order_id}:{h}"


def adjusted_key(tracking_id: int) -> str:
    return f"stock.adjusted:{tracking_id}"


def part_created_key(part_id: int) -> str:
    return f"part.created:{part_id}"


def build_stock_received(tenant: str, po_pk: int, po_reference: str, supplier: str | None, lines: list[dict],
                         occurred_at: datetime | None = None) -> dict:
    return envelope("stock.received", tenant, {"kind": "purchase_order", "id": po_reference, "ref": str(po_pk)},
                    {"purchase_order": po_reference, "supplier": supplier, "lines": lines}, occurred_at)


def build_stock_adjusted(tenant: str, item_pk: int, part_ref: str, delta, reason: str, quantity_after, tracking_id: int,
                         location: str | None = None, occurred_at: datetime | None = None) -> dict:
    return envelope("stock.adjusted", tenant, {"kind": "stock_item", "id": str(item_pk), "ref": str(item_pk)},
                    {"part_ref": part_ref, "delta": dec_str(delta), "reason": reason,
                     "quantity_after": dec_str(quantity_after), "tracking_id": tracking_id, "location": location},
                    occurred_at)


def build_part_created(tenant: str, part_pk: int, part_ref: str, name: str, ipn: str | None, category: str | None,
                       units: str | None, occurred_at: datetime | None = None) -> dict:
    return envelope("part.created", tenant, {"kind": "part", "id": part_ref, "ref": str(part_pk)},
                    {"part_ref": part_ref, "name": name, "ipn": ipn, "category": category, "units": units}, occurred_at)
