from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Iterable


GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
GITHUB_REPO = os.getenv("GITHUB_REPO", "alpaca-trade-logs").strip()
GITHUB_OWNER = os.getenv("GITHUB_OWNER", "").strip()
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main").strip()
GIT_AUTHOR_NAME = os.getenv("GIT_AUTHOR_NAME", "stock-scanner-bot").strip()
GIT_AUTHOR_EMAIL = os.getenv("GIT_AUTHOR_EMAIL", "stock-scanner-bot@example.com").strip()


class GitHubExportConfigurationError(RuntimeError):
    """Raised when GitHub export settings are missing or invalid."""


def _run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\n"
            f"stdout={result.stdout.strip()}\n"
            f"stderr={result.stderr.strip()}"
        )
    return result


def _masked_remote_url() -> str:
    if not GITHUB_OWNER:
        raise GitHubExportConfigurationError("Missing required GitHub setting: GITHUB_OWNER")
    if not GITHUB_TOKEN:
        raise GitHubExportConfigurationError("Missing required GitHub setting: GITHUB_TOKEN")
    if not GITHUB_REPO:
        raise GitHubExportConfigurationError("Missing required GitHub setting: GITHUB_REPO")
    return f"https://x-access-token:{GITHUB_TOKEN}@github.com/{GITHUB_OWNER}/{GITHUB_REPO}.git"


def _prepare_repo(path: Path) -> Path:
    if path.exists():
        shutil.rmtree(path)

    path.parent.mkdir(parents=True, exist_ok=True)

    remote_url = _masked_remote_url()
    clone_parent = path.parent
    _run(["git", "clone", "--branch", GITHUB_BRANCH, remote_url, str(path.name)], clone_parent)

    _run(["git", "config", "user.name", GIT_AUTHOR_NAME], path)
    _run(["git", "config", "user.email", GIT_AUTHOR_EMAIL], path)
    _run(["git", "remote", "set-url", "origin", remote_url], path)

    return path


def commit_and_push(repo_dir: str | Path, files_to_add: Iterable[str | Path], message: str) -> None:
    path = _prepare_repo(Path(repo_dir).expanduser().resolve())

    add_targets = [Path(file_path) for file_path in files_to_add]
    if not add_targets:
        raise ValueError("files_to_add must not be empty")

    for src in add_targets:
        src_path = src.expanduser().resolve()
        if not src_path.exists():
            continue

        dst_path = path / src_path.name
        if src_path.is_dir():
            if dst_path.exists():
                shutil.rmtree(dst_path)
            shutil.copytree(src_path, dst_path)
        else:
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_path, dst_path)

    _run(["git", "add", "."], path)

    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=str(path),
        check=False,
        text=True,
        capture_output=True,
    )

    if not status.stdout.strip():
        return

    _run(["git", "commit", "-m", message], path)
    _run(["git", "push", "origin", GITHUB_BRANCH], path)