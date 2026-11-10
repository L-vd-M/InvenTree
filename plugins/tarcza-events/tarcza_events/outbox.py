# SPDX-License-Identifier: AGPL-3.0-or-later
"""Outbox table (raw SQL, no Django migrations: survives InvenTree upgrades and plugin reloads) and delivery.

Rows are written inside the caller's transaction. Delivery is at least once: the webhook-id is fixed
at enqueue time, the signature timestamp is fresh on every attempt, consumers de-duplicate on webhook-id.
"""

import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta

from django.db import connection, transaction

from . import core

DDL = """
CREATE TABLE IF NOT EXISTS tarcza_outbox (
    id BIGSERIAL PRIMARY KEY,
    dedup_key TEXT NOT NULL UNIQUE,
    msg_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at TIMESTAMPTZ NULL,
    dead_at TIMESTAMPTZ NULL,
    last_error TEXT NULL
);
CREATE TABLE IF NOT EXISTS tarcza_state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS tarcza_outbox_due ON tarcza_outbox (next_attempt_at)
    WHERE delivered_at IS NULL AND dead_at IS NULL;
"""

_schema_ready = False


def ensure_schema():
    """Create the outbox tables once per process."""
    global _schema_ready
    if _schema_ready:
        return
    with connection.cursor() as cur:
        cur.execute(DDL)
    _schema_ready = True


def state_get_or_set(key: str, default: str) -> str:
    """Return a stored state value, storing the default on first use."""
    ensure_schema()
    with connection.cursor() as cur:
        cur.execute(
            'INSERT INTO tarcza_state (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING',
            [key, default],
        )
        cur.execute('SELECT value FROM tarcza_state WHERE key = %s', [key])
        return cur.fetchone()[0]


def enqueue(dedup_key: str, event: dict) -> bool:
    """Insert once per dedup_key (inside the caller's transaction). True if a new row was written."""
    ensure_schema()
    with connection.cursor() as cur:
        cur.execute(
            'INSERT INTO tarcza_outbox (dedup_key, msg_id, event_type, body) VALUES (%s, %s, %s, %s) '
            'ON CONFLICT (dedup_key) DO NOTHING',
            [
                dedup_key,
                core.new_msg_id(),
                event['type'],
                core.body_bytes(event).decode(),
            ],
        )
        return cur.rowcount == 1


def post(url: str, secret: str, msg_id: str, body: bytes, timeout: int = 10) -> int:
    """POST one signed event; return the HTTP status."""
    req = urllib.request.Request(
        url,
        data=body,
        headers=core.delivery_headers(secret, msg_id, body),
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


def deliver_due(url: str, secret: str, limit: int = 50, poster=post) -> dict:
    """Deliver due rows one at a time under FOR UPDATE SKIP LOCKED. Returns counts."""
    ensure_schema()
    out = {'delivered': 0, 'failed': 0, 'dead': 0}
    for _ in range(limit):
        with transaction.atomic(), connection.cursor() as cur:
            cur.execute(
                'SELECT id, msg_id, body, attempts, created_at FROM tarcza_outbox '
                'WHERE delivered_at IS NULL AND dead_at IS NULL AND next_attempt_at <= now() '
                'ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED'
            )
            row = cur.fetchone()
            if row is None:
                break
            rid, msg_id, body, attempts, created_at = row
            try:
                status, err = poster(url, secret, msg_id, body.encode()), None
            except Exception as exc:  # network error: treated as a failed attempt
                status, err = 0, repr(exc)
            if 200 <= status < 300:
                cur.execute(
                    'UPDATE tarcza_outbox SET delivered_at = now(), attempts = attempts + 1, last_error = NULL WHERE id = %s',
                    [rid],
                )
                out['delivered'] += 1
                continue
            attempts += 1
            err = err or f'HTTP {status}'
            now = datetime.now(UTC)
            if core.is_dead(created_at, now):
                cur.execute(
                    'UPDATE tarcza_outbox SET dead_at = now(), attempts = %s, last_error = %s WHERE id = %s',
                    [attempts, err, rid],
                )
                out['dead'] += 1
            else:
                nxt = now + timedelta(seconds=core.next_delay(attempts))
                cur.execute(
                    'UPDATE tarcza_outbox SET attempts = %s, next_attempt_at = %s, last_error = %s WHERE id = %s',
                    [attempts, nxt, err, rid],
                )
                out['failed'] += 1
    return out


def stats() -> dict:
    """Return pending, delivered and dead counts."""
    ensure_schema()
    with connection.cursor() as cur:
        cur.execute(
            'SELECT count(*) FILTER (WHERE delivered_at IS NULL AND dead_at IS NULL), '
            'count(*) FILTER (WHERE delivered_at IS NOT NULL), count(*) FILTER (WHERE dead_at IS NOT NULL) FROM tarcza_outbox'
        )
        pending, delivered, dead = cur.fetchone()
    return {'pending': pending, 'delivered': delivered, 'dead': dead}
