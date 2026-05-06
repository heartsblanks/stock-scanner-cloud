import unittest
from unittest.mock import patch

from app import _post_sync_repair_policy


class AppSyncRepairPolicyTests(unittest.TestCase):
    def test_repairs_after_every_sync_by_default(self):
        with patch.dict("os.environ", {}, clear=False):
            self.assertEqual(_post_sync_repair_policy({"ok": True}), (True, "after_sync"))

    def test_can_fall_back_to_partial_sync_only(self):
        with patch.dict(
            "os.environ",
            {
                "AUTO_IBKR_REPAIR_AFTER_SYNC": "false",
                "AUTO_IBKR_REPAIR_AFTER_PARTIAL_SYNC": "true",
            },
            clear=False,
        ):
            self.assertEqual(
                _post_sync_repair_policy({"ok": False, "partial": True, "stopped_reason": "ibkr_timeout"}),
                (True, "after_partial_sync"),
            )
            self.assertEqual(_post_sync_repair_policy({"ok": True}), (False, ""))

    def test_can_disable_post_sync_repair(self):
        with patch.dict(
            "os.environ",
            {
                "AUTO_IBKR_REPAIR_AFTER_SYNC": "false",
                "AUTO_IBKR_REPAIR_AFTER_PARTIAL_SYNC": "false",
            },
            clear=False,
        ):
            self.assertEqual(
                _post_sync_repair_policy({"ok": False, "partial": True, "stopped_reason": "ibkr_timeout"}),
                (False, ""),
            )


if __name__ == "__main__":
    unittest.main()
