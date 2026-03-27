

from __future__ import annotations

import os
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



def _run(cmd: list[str], cwd: Path) -> None:
    subprocess.run(cmd, cwd=str(cwd), check=True)



def _masked_remote_url() -> str:
    if not GITHUB_OWNER:
        raise GitHubExportConfigurationError("Missing required GitHub setting: GITHUB_OWNER")
    if not GITHUB_TOKEN:
        raise GitHubExportConfigurationError("Missing required GitHub setting: GITHUB_TOKEN")
    if not GITHUB_REPO:
        raise GitHubExportConfigurationError("Missing required GitHub setting: GITHUB_REPO")
    return f"https://{GITHUB_TOKEN}@github.com/{GITHUB_OWNER}/{GITHUB_REPO}.git"



def init_repo(repo_dir: str | Path) -> Path:
    path = Path(repo_dir).expanduser().resolve()
    path.mkdir(parents=True, exist_ok=True)

    if not (path / ".git").exists():
        _run(["git", "init"], path)

    _run(["git", "config", "user.name", GIT_AUTHOR_NAME], path)
    _run(["git", "config", "user.email", GIT_AUTHOR_EMAIL], path)

    remote_url = _masked_remote_url()
    remotes = subprocess.run(
        ["git", "remote"],
        cwd=str(path),
        check=True,
        text=True,
        capture_output=True,
    ).stdout.splitlines()

    if "origin" in remotes:
        _run(["git", "remote", "set-url", "origin", remote_url], path)
    else:
        _run(["git", "remote", "add", "origin", remote_url], path)

    return path



def commit_and_push(repo_dir: str | Path, files_to_add: Iterable[str | Path], message: str) -> None:
    path = init_repo(repo_dir)

    add_targets = [str(Path(file_path)) for file_path in files_to_add]
    if not add_targets:
        raise ValueError("files_to_add must not be empty")

    _run(["git", "add", *add_targets], path)

    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=str(path),
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()

    if not status:
        return

    _run(["git", "commit", "-m", message], path)
    _run(["git", "branch", "-M", GITHUB_BRANCH], path)
    _run(["git", "push", "-u", "origin", GITHUB_BRANCH], path)