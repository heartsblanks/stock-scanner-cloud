from __future__ import annotations

import base64
import os
import sys
from pathlib import Path
from typing import Iterable

import requests

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
GITHUB_REPO = os.getenv("GITHUB_REPO", "stock-scanner-trade-logs").strip()
GITHUB_OWNER = os.getenv("GITHUB_OWNER", "").strip()
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main").strip()
GIT_AUTHOR_NAME = os.getenv("GIT_AUTHOR_NAME", "stock-scanner-bot").strip()
GIT_AUTHOR_EMAIL = os.getenv("GIT_AUTHOR_EMAIL", "stock-scanner-bot@example.com").strip()
GITHUB_API_BASE = "https://api.github.com"


class GitHubExportConfigurationError(RuntimeError):
    """Raised when GitHub export settings are missing or invalid."""


def _validate_config() -> None:
    if not GITHUB_OWNER:
        raise GitHubExportConfigurationError("Missing required GitHub setting: GITHUB_OWNER")
    if not GITHUB_TOKEN:
        raise GitHubExportConfigurationError("Missing required GitHub setting: GITHUB_TOKEN")
    if not GITHUB_REPO:
        raise GitHubExportConfigurationError("Missing required GitHub setting: GITHUB_REPO")


def _headers() -> dict[str, str]:
    _validate_config()
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _contents_url(path: str) -> str:
    return f"{GITHUB_API_BASE}/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{path}"


def _get_existing_file(path: str) -> dict[str, object] | None:
    response = requests.get(
        _contents_url(path),
        headers=_headers(),
        params={"ref": GITHUB_BRANCH},
        timeout=30,
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def _decode_existing_content(payload: dict[str, object]) -> bytes:
    encoded = str(payload.get("content", "")).replace("\n", "")
    if not encoded:
        return b""
    return base64.b64decode(encoded)


def _upsert_file(path: str, content: bytes, message: str) -> bool:
    existing = _get_existing_file(path)
    sha = None
    if existing is not None:
        if _decode_existing_content(existing) == content:
            return False
        sha = existing.get("sha")

    payload: dict[str, object] = {
        "message": message,
        "content": base64.b64encode(content).decode("ascii"),
        "branch": GITHUB_BRANCH,
        "committer": {
            "name": GIT_AUTHOR_NAME,
            "email": GIT_AUTHOR_EMAIL,
        },
    }
    if sha:
        payload["sha"] = sha

    response = requests.put(
        _contents_url(path),
        headers=_headers(),
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    return True


def commit_and_push(
    repo_dir: str | Path,
    files_to_add: Iterable[str | Path],
    message: str,
    source_base_dir: str | Path,
) -> None:
    del repo_dir  # Kept for interface compatibility with existing callers.

    source_base = Path(source_base_dir).expanduser().resolve()
    add_targets = [Path(file_path) for file_path in files_to_add]
    if not add_targets:
        raise ValueError("files_to_add must not be empty")

    changed = False

    for relative_path in add_targets:
        src_path = source_base / relative_path
        if not src_path.exists() or src_path.is_dir():
            continue

        changed = _upsert_file(
            str(relative_path).replace(os.sep, "/"),
            src_path.read_bytes(),
            message,
        ) or changed

    if not changed:
        return
