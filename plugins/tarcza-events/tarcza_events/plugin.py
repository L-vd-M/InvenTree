# SPDX-License-Identifier: AGPL-3.0-or-later
"""InvenTree plugin: turns InvenTree events into contract events through a transactional outbox.

InvenTree events are asynchronous (register_event / process_event run in the background worker), so
process_event is retried by the worker on error and may run more than once: every outbox write is
keyed (dedup_key) and therefore idempotent.
"""

import os
from decimal import Decimal

from django.db import transaction

import structlog

from plugin import InvenTreePlugin
from plugin.mixins import EventMixin, ScheduleMixin, SettingsMixin

from . import __version__, core, outbox

logger = structlog.get_logger('inventree')

EV_RECEIVED = 'purchaseorderitem.received'
EV_QTY = 'stockitem.quantityupdated'
EV_COUNTED = 'stockitem.counted'
EV_TRACKING = 'stock_stockitemtracking.created'
EV_PART = 'part_part.created'
ADJUST_TRIGGERS = {EV_QTY, EV_COUNTED, EV_TRACKING}
WANTED = {EV_RECEIVED, EV_PART} | ADJUST_TRIGGERS

# StockHistoryCode values: 10 count, 11 manual add, 12 manual remove
ADJUST_REASONS = {10: 'count', 11: 'add', 12: 'remove'}


def _setting(plugin, env, key, default=''):
    return os.environ.get(env) or plugin.get_setting(key) or default


class TarczaEventsPlugin(EventMixin, ScheduleMixin, SettingsMixin, InvenTreePlugin):
    """Emit TARCZA contract events through a transactional outbox."""

    NAME = 'TarczaEvents'
    SLUG = 'tarcza-events'
    TITLE = 'TARCZA events (outbox + Standard Webhooks)'
    DESCRIPTION = (
        'Emits stock.received, stock.adjusted and part.created to the TARCZA hub.'
    )
    VERSION = __version__
    AUTHOR = 'TARCZA'
    LICENSE = 'AGPL-3.0-or-later'

    SETTINGS = {
        'TENANT': {
            'name': 'Tenant',
            'description': 'Tenant slug carried in every event (env TARCZA_TENANT overrides)',
            'default': 'tarcza',
        },
        'CONSUMER_URL': {
            'name': 'Consumer URL',
            'description': 'Event receiver, e.g. https://hub/events (env TARCZA_EVENT_CONSUMER_URL overrides)',
            'default': '',
        },
        'SECRET': {
            'name': 'Shared secret',
            'description': 'whsec_ secret (prefer env TARCZA_EVENT_SECRET)',
            'default': '',
            'protected': True,
        },
    }

    SCHEDULED_TASKS = {
        'deliver': {'func': 'deliver_outbox', 'schedule': 'I', 'minutes': 1}
    }

    # config

    def tenant(self):
        """Tenant slug carried in every event."""
        return _setting(self, 'TARCZA_TENANT', 'TENANT', 'tarcza')

    def consumer_url(self):
        """Event receiver URL."""
        return _setting(self, 'TARCZA_EVENT_CONSUMER_URL', 'CONSUMER_URL')

    def secret(self):
        """Shared Standard Webhooks secret."""
        return _setting(self, 'TARCZA_EVENT_SECRET', 'SECRET')

    # events

    def wants_process_event(self, event: str) -> bool:
        """Subscribe to the InvenTree events this plugin turns into contract events."""
        return event in WANTED

    def process_event(self, event: str, *args, **kwargs) -> None:
        """Handle one InvenTree event (runs in the background worker, may repeat)."""
        with transaction.atomic():
            if event == EV_RECEIVED:
                key, ev = self._received(kwargs)
            elif event in ADJUST_TRIGGERS:
                self.sweep_adjustments(first_run_lookback_minutes=10)
                return
            elif event == EV_PART:
                key, ev = self._part_created(kwargs)
            else:
                return
            if ev is None:
                return
            new = outbox.enqueue(key, ev)
        if new:
            transaction.on_commit(self.deliver_outbox)

    def _received(self, kw):
        from order.models import PurchaseOrder
        from stock.models import StockItem

        order_id, item_ids = kw['order_id'], kw['item_ids']
        po = PurchaseOrder.objects.get(pk=order_id)
        lines = []
        for item in (
            StockItem.objects
            .filter(pk__in=item_ids)
            .select_related('part', 'supplier_part', 'location')
            .order_by('pk')
        ):
            lines.append({
                'stock_item': item.pk,
                'part_ref': item.part.IPN or str(item.part.pk),
                'part_name': item.part.name,
                'quantity': core.dec_str(item.quantity),
                'unit_cost': core.money(
                    item.purchase_price, item.purchase_price_currency
                ),
                'supplier_sku': item.supplier_part.SKU if item.supplier_part else None,
                'location': item.location.pathstring if item.location else None,
            })
        if not lines:
            return None, None
        ev = core.build_stock_received(
            self.tenant(),
            po.pk,
            po.reference,
            po.supplier.name if po.supplier else None,
            lines,
        )
        return core.received_key(po.pk, item_ids), ev

    def sweep_adjustments(self, first_run_lookback_minutes=0):
        """Emit stock.adjusted for every count/add/remove history entry since the plugin was first swept.

        The tracking table is the source of truth: the API writes history through bulk_create (no per-row
        signal) and InvenTree fires quantityupdated and counted separately, so events only trigger this
        sweep. The dedup key (tracking id) makes any number of sweeps idempotent.
        """
        from datetime import timedelta

        from django.utils import timezone

        from stock.models import StockItemTracking

        outbox.ensure_schema()
        since = outbox.state_get_or_set(
            'adjust_since',
            core.iso_utc(
                timezone.now() - timedelta(minutes=first_run_lookback_minutes)
            ),
        )
        floor = max(core.parse_iso(since), timezone.now() - timedelta(days=1))
        qs = (
            StockItemTracking.objects
            .filter(tracking_type__in=list(ADJUST_REASONS), date__gte=floor)
            .select_related('item', 'part', 'item__location')
            .order_by('pk')
        )
        n = 0
        for tr in qs:
            if tr.item is None:
                continue
            ev = self._adjusted_event(tr)
            if outbox.enqueue(core.adjusted_key(tr.pk), ev):
                n += 1
        return n

    def _adjusted_event(self, tr):
        from stock.models import StockItemTracking

        d = tr.deltas or {}
        after = Decimal(str(d.get('quantity', tr.item.quantity)))
        if tr.tracking_type == 11:
            delta = Decimal(str(d.get('added', 0)))
        elif tr.tracking_type == 12:
            delta = -Decimal(str(d.get('removed', 0)))
        else:
            prev = (
                StockItemTracking.objects
                .filter(item=tr.item, pk__lt=tr.pk, deltas__has_key='quantity')
                .order_by('-pk')
                .first()
            )
            before = (
                Decimal(str((prev.deltas or {}).get('quantity', 0)))
                if prev
                else Decimal(0)
            )
            delta = after - before
        part = tr.part or tr.item.part
        return core.build_stock_adjusted(
            self.tenant(),
            tr.item.pk,
            part.IPN or str(part.pk),
            delta,
            ADJUST_REASONS[tr.tracking_type],
            after,
            tr.pk,
            tr.item.location.pathstring if tr.item.location else None,
            tr.date,
        )

    def _part_created(self, kw):
        from part.models import Part

        p = Part.objects.select_related('category').filter(pk=kw['id']).first()
        if p is None:
            return None, None
        ref = p.IPN or str(p.pk)
        ev = core.build_part_created(
            self.tenant(),
            p.pk,
            ref,
            p.name,
            p.IPN or None,
            p.category.pathstring if p.category else None,
            p.units or None,
        )
        return core.part_created_key(p.pk), ev

    # delivery

    def deliver_outbox(self, *args, **kwargs):
        """Scheduled task: sweep stock history, then deliver due outbox rows."""
        with transaction.atomic():
            self.sweep_adjustments()
        url, secret = self.consumer_url(), self.secret()
        if not url or not secret:
            return {'skipped': 'consumer url or secret not configured'}
        res = outbox.deliver_due(url, secret)
        if res['failed'] or res['dead']:
            logger.warning('tarcza-events delivery: %s', res)
        return res
