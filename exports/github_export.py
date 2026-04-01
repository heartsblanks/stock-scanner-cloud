from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


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


def _clone_repo(repo_dir: Path) -> Path:
    if repo_dir.exists():
        shutil.rmtree(repo_dir)

    repo_dir.parent.mkdir(parents=True, exist_ok=True)

    remote_url = _masked_remote_url()
    _run(
        ["git", "clone", "--branch", GITHUB_BRANCH, remote_url, str(repo_dir.name)],
        repo_dir.parent,
    )

    _run(["git", "config", "user.name", GIT_AUTHOR_NAME], repo_dir)
    _run(["git", "config", "user.email", GIT_AUTHOR_EMAIL], repo_dir)
    _run(["git", "remote", "set-url", "origin", remote_url], repo_dir)

    return repo_dir


def commit_and_push(
    repo_dir: str | Path,
    files_to_add: Iterable[str | Path],
    message: str,
    source_base_dir: str | Path,
) -> None:
    repo_path = _clone_repo(Path(repo_dir).expanduser().resolve())
    source_base = Path(source_base_dir).expanduser().resolve()

    add_targets = [Path(file_path) for file_path in files_to_add]
    if not add_targets:
        raise ValueError("files_to_add must not be empty")

    copied_targets: list[str] = []

    for relative_path in add_targets:
        src_path = source_base / relative_path
        dst_path = repo_path / relative_path

        if not src_path.exists():
            continue

        dst_path.parent.mkdir(parents=True, exist_ok=True)

        if src_path.is_dir():
            if dst_path.exists():
                shutil.rmtree(dst_path)
            shutil.copytree(src_path, dst_path)
        else:
            shutil.copy2(src_path, dst_path)

        copied_targets.append(str(relative_path))

    if not copied_targets:
        return

    _run(["git", "add", *copied_targets], repo_path)

    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=str(repo_path),
        check=False,
        text=True,
        capture_output=True,
    ).stdout.strip()

    if not status:
        return

    _run(["git", "commit", "-m", message], repo_path)
    _run(["git", "push", "origin", GITHUB_BRANCH], repo_path)
