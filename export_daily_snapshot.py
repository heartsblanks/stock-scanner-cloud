from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from export_reports import export_all_reports
from github_export import commit_and_push


EXPORT_BASE_DIR = os.getenv("EXPORT_BASE_DIR", "/tmp/alpaca-trade-logs-export")
EXPORT_REPO_DIR = os.getenv("EXPORT_REPO_DIR", "/tmp/alpaca-trade-logs-repo")


def run_daily_snapshot() -> dict[str, object]:
    run_time = datetime.now(timezone.utc)
    target_date = run_time.date()

    export_base_dir = Path(EXPORT_BASE_DIR)
    repo_dir = Path(EXPORT_REPO_DIR)
    export_dir = export_base_dir / "daily_exports"

    exported_files = export_all_reports(export_dir, for_date=target_date)

    relative_files = [file_path.relative_to(export_base_dir) for file_path in exported_files]
    commit_message = f"daily snapshot {target_date.isoformat()}"

    commit_and_push(
        repo_dir=repo_dir,
        files_to_add=relative_files,
        message=commit_message,
        source_base_dir=export_base_dir,
    )

    return {
        "ok": True,
        "run_time_utc": run_time.isoformat(),
        "target_date": target_date.isoformat(),
        "repo_dir": str(repo_dir),
        "export_base_dir": str(export_base_dir),
        "exported_files": [str(path) for path in relative_files],
        "commit_message": commit_message,
    }


if __name__ == "__main__":
    result = run_daily_snapshot()
    print(result)