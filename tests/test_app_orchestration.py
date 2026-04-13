import unittest
from datetime import UTC, datetime

from orchestration.app_orchestration import build_reconcile_now_response


class AppOrchestrationTests(unittest.TestCase):
    def test_build_reconcile_now_response_uploads_with_positional_signature(self):
        captured_upload_args = []
        captured_run_args = []

        result = build_reconcile_now_response(
            run_reconciliation=lambda: {
                "ok": True,
                "mismatch_count": 1,
                "total_rows": 5,
                "severity": "WARNING",
                "file_path": "alpaca_reconciliation.csv",
            },
            upload_file_to_gcs=lambda local_path, bucket_name, object_name: captured_upload_args.append(
                (local_path, bucket_name, object_name)
            ) or "gs://bucket/reconciliation.csv",
            reconciliation_bucket="bucket-name",
            reconciliation_object="reconciliation.csv",
            safe_insert_reconciliation_run=lambda **kwargs: captured_run_args.append(kwargs),
        )

        self.assertTrue(result["ok"])
        self.assertEqual(
            captured_upload_args,
            [("alpaca_reconciliation.csv", "bucket-name", "reconciliation.csv")],
        )
        self.assertEqual(len(captured_run_args), 1)
        self.assertEqual(captured_run_args[0]["matched_count"], 4)
        self.assertEqual(captured_run_args[0]["unmatched_count"], 1)
        self.assertIsInstance(captured_run_args[0]["run_time"], datetime)
        self.assertEqual(captured_run_args[0]["run_time"].tzinfo, UTC)


if __name__ == "__main__":
    unittest.main()
