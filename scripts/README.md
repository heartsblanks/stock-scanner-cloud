# Scripts

One-off operational utilities live here instead of the repository root.

Current contents:
- `backfill_trade_lifecycles.py`
- `cleanup_open_trade_lifecycles_from_broker_orders.py`
- `repair_ibkr_stale_closes_from_vm_logs.py`
- `repair_trade_lifecycle_directions.py`

These scripts add the repository root to `sys.path` so they can still import the active runtime modules when executed directly with:

```bash
python3 scripts/<script_name>.py
```
