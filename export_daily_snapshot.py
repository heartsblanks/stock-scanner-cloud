

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from export_reports import export_all_reports
from github_export import commit_and_push


EXPORT_BASE_DIR = os.getenv("EXPORT_BASE_DIR", "/tmp/alpaca-trade-logs")
EXPORT_REPO_DIR = os.getenv("EXPORT_REPO_DIR", EXPORT_BASE_DIR)


def run_daily_snapshot() -> dict[str, object]:
    run_time = datetime.now(timezone.utc)
    target_date = run_time.date()

    repo_dir = Path(EXPORT_REPO_DIR)
    export_dir = repo_dir / "daily_exports"

    exported_files = export_all_reports(export_dir, for_date=target_date)

    relative_files = [file_path.relative_to(repo_dir) for file_path in exported_files]
    commit_message = f"daily snapshot {target_date.isoformat()}"

    commit_and_push(
        repo_dir=repo_dir,
        files_to_add=relative_files,
        message=commit_message,
    )

    return {
        "ok": True,
        "run_time_utc": run_time.isoformat(),
        "target_date": target_date.isoformat(),
        "repo_dir": str(repo_dir),
        "exported_files": [str(path) for path in relative_files],
        "commit_message": commit_message,
    }


if __name__ == "__main__":
    result = run_daily_snapshot()
    print(result)