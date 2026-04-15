import unittest

try:
    from flask import Flask
except ModuleNotFoundError as exc:
    raise unittest.SkipTest("flask is not available in this local unittest environment") from exc

from routes.trades import register_trade_routes


class TradeRoutesTests(unittest.TestCase):
    def _build_client(self):
        app = Flask(__name__)
        register_trade_routes(
            app,
            append_trade_log=lambda payload: None,
            safe_insert_trade_event=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            close_all_paper_positions=lambda: {"ok": True},
            read_trade_rows_for_date=lambda target_date: [],
            find_instrument_by_symbol=lambda symbol: ("Name", "mode"),
            find_best_signal_match=lambda symbol, price, timestamp: None,
            find_latest_open_trade=lambda symbol, **kwargs: None,
            infer_first_level_hit=lambda *args, **kwargs: "",
            to_float_or_none=lambda value: float(value) if value not in (None, "", "None") else None,
            parse_iso_utc=lambda value: value,
            get_open_trade_events=lambda limit=100: [],
            get_closed_trade_events=lambda limit=100: [],
            get_recent_trade_event_rows=lambda limit=100, broker=None: [],
            get_latest_scan_summary=lambda: {},
            get_trade_lifecycles=lambda limit=100, status=None, broker=None: [
                {
                    "symbol": "QBTS",
                    "broker": "IBKR",
                    "status": "OPEN",
                    "mode": "fourth",
                    "side": "BUY",
                    "direction": "LONG",
                    "shares": "345",
                    "entry_price": "14.49",
                    "stop_price": "14.266",
                    "target_price": "14.938",
                    "parent_order_id": "88",
                    "order_id": "88",
                }
            ],
            get_trade_lifecycle_summary_from_table=lambda limit=1000, broker=None: [],
            get_open_positions_for_broker_name=lambda broker_name: [
                {
                    "symbol": "QBTS",
                    "qty": 345.0,
                    "avg_entry_price": 14.525,
                    "current_price": 17.75,
                    "market_value": 6123.75,
                    "unrealized_pl": 1115.0,
                    "side": "long",
                }
            ] if broker_name == "IBKR" else [],
            get_open_orders_for_broker_name=lambda broker_name: [
                {
                    "id": "188",
                    "symbol": "QBTS",
                    "side": "sell",
                    "type": "lmt",
                    "status": "Submitted",
                    "parent_id": "88",
                    "limit_price": 18.69,
                    "stop_price": 0.0,
                },
                {
                    "id": "189",
                    "symbol": "QBTS",
                    "side": "sell",
                    "type": "stp",
                    "status": "Submitted",
                    "parent_id": "88",
                    "limit_price": 0.0,
                    "stop_price": 16.69,
                },
            ] if broker_name == "IBKR" else [],
            upsert_trade_lifecycle=lambda **kwargs: None,
        )
        return app.test_client()

    def test_open_trades_returns_stored_and_live_ibkr_fields(self):
        client = self._build_client()

        response = client.get("/open-trades?broker=IBKR")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["ibkr_live_available"])
        row = payload["rows"][0]
        self.assertEqual(row["symbol"], "QBTS")
        self.assertEqual(row["stored_entry_price"], 14.49)
        self.assertEqual(row["live_entry_price"], 14.525)
        self.assertEqual(row["live_current_price"], 17.75)
        self.assertEqual(row["live_target_price"], 18.69)
        self.assertEqual(row["live_stop_price"], 16.69)
        self.assertTrue(row["entry_price_mismatch"])
        self.assertTrue(row["stop_price_mismatch"])
        self.assertTrue(row["target_price_mismatch"])


if __name__ == "__main__":
    unittest.main()
