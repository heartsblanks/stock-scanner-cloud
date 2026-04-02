import base64
import tempfile
import unittest
from pathlib import Path
from unittest import SkipTest
from unittest.mock import MagicMock, patch

try:
    from exports import github_export
except ModuleNotFoundError as exc:
    if exc.name == "requests":
        raise SkipTest("requests dependency is not available in this local unittest environment")
    raise


class GitHubExportTests(unittest.TestCase):
    @patch("exports.github_export.requests.put")
    @patch("exports.github_export.requests.get")
    def test_commit_and_push_uses_github_contents_api(self, mock_get, mock_put):
        mock_get_response = MagicMock()
        mock_get_response.status_code = 404
        mock_get.return_value = mock_get_response

        mock_put_response = MagicMock()
        mock_put.return_value = mock_put_response

        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            target = base_dir / "daily_exports" / "2026-04-02" / "db" / "scan_runs.csv"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("id,value\n1,test\n", encoding="utf-8")

            with patch.object(github_export, "GITHUB_OWNER", "heartsblanks"), patch.object(
                github_export, "GITHUB_REPO", "alpaca-trade-logs"
            ), patch.object(github_export, "GITHUB_TOKEN", "token"), patch.object(
                github_export, "GITHUB_BRANCH", "main"
            ):
                github_export.commit_and_push(
                    repo_dir="/tmp/unused",
                    files_to_add=[target.relative_to(base_dir)],
                    message="daily snapshot 2026-04-02",
                    source_base_dir=base_dir,
                )

        mock_get.assert_called_once()
        mock_put.assert_called_once()
        payload = mock_put.call_args.kwargs["json"]
        self.assertEqual(payload["message"], "daily snapshot 2026-04-02")
        self.assertEqual(payload["branch"], "main")
        self.assertEqual(
            base64.b64decode(payload["content"]).decode("utf-8"),
            "id,value\n1,test\n",
        )


if __name__ == "__main__":
    unittest.main()
