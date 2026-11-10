# tarcza-events

InvenTree plugin (AGPL-3.0-or-later, TARCZA). Writes contract events to a transactional outbox (`tarcza_outbox`, raw SQL, same Postgres) and delivers them with Standard Webhooks (HMAC-SHA256, `webhook-id`/`-timestamp`/`-signature`), exponential backoff 30 s to 6 h, dead after 72 h.

Events (contracts v1.1): `stock.received` (PO receive), `stock.adjusted` (count/add/remove), `part.created`.
Config (env wins over plugin settings): `TARCZA_TENANT`, `TARCZA_EVENT_CONSUMER_URL`, `TARCZA_EVENT_SECRET` (`whsec_...`).
Install: `-e /home/inventree/tarcza-plugins/tarcza-events` in `plugins.txt`; enable the plugin and global settings `ENABLE_PLUGINS_EVENTS` and `ENABLE_PLUGINS_SCHEDULE`. Design: wrapper repo `docs/adr/0001-events-plugin-design.md`. Tests: wrapper repo `tests/` and `tests/e2e/events_e2e.py`.
