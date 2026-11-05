"""Resolve an Application's source (git repo + revision + path) to a local directory."""
from __future__ import annotations

import hashlib
import os
import subprocess
import tarfile
import tempfile
import io
from dataclasses import dataclass

from .manifest import Application


class SourceError(RuntimeError):
    pass


@dataclass
class Checkout:
    directory: str      # directory containing the manifests (repo root joined with app path)
    revision: str       # full git sha, or "local-<hash>" for non-git directories
    _tmp: tempfile.TemporaryDirectory | None = None

    def cleanup(self):
        if self._tmp:
            self._tmp.cleanup()


def _git(*args: str, cwd: str | None = None) -> str:
    try:
        r = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True,
                           env={**os.environ, "GIT_TERMINAL_PROMPT": "0"})
    except FileNotFoundError:
        raise SourceError("git is not installed") from None
    if r.returncode != 0:
        raise SourceError(f"git {' '.join(args)} failed: {r.stderr.strip()}")
    return r.stdout.strip()


def _is_git_dir(path: str) -> bool:
    return os.path.isdir(path) and subprocess.run(
        ["git", "rev-parse", "--git-dir"], cwd=path, capture_output=True).returncode == 0


def _hash_tree(directory: str) -> str:
    h = hashlib.sha256()
    for root, dirs, files in os.walk(directory):
        dirs[:] = sorted(d for d in dirs if not d.startswith("."))
        for fn in sorted(files):
            p = os.path.join(root, fn)
            h.update(os.path.relpath(p, directory).encode())
            with open(p, "rb") as f:
                h.update(f.read())
    return "local-" + h.hexdigest()[:12]


def resolve(app: Application, cache_dir: str, base_dir: str = ".") -> Checkout:
    """Fetch app.repo_url at app.target_revision and return a Checkout of app.path.

    A repoURL that is an existing non-git directory is used as-is (handy for demos).
    Relative local paths resolve against `base_dir` (the Application file's directory).
    """
    repo = app.repo_url
    local = os.path.normpath(os.path.join(base_dir, repo)) if "://" not in repo and not repo.startswith("git@") else None
    if local and os.path.isdir(local) and not _is_git_dir(local):
        d = os.path.join(local, app.path)
        if not os.path.isdir(d):
            raise SourceError(f"path {app.path!r} not found in {local}")
        return Checkout(d, _hash_tree(d))

    url = local if local else repo
    os.makedirs(cache_dir, exist_ok=True)
    mirror = os.path.join(cache_dir, hashlib.sha256(url.encode()).hexdigest()[:16] + ".git")
    if not os.path.isdir(mirror):
        _git("clone", "--bare", "--quiet", url, mirror)
    # Mirror everything so branches, tags and arbitrary shas all resolve.
    _git("fetch", "--quiet", "--prune", "--force", url,
         "+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*", cwd=mirror)
    rev = app.target_revision
    sha = None
    for cand in ([rev] if rev != "HEAD" else ["HEAD"]):
        try:
            sha = _git("rev-parse", "--verify", "--quiet", f"{cand}^{{commit}}", cwd=mirror)
            break
        except SourceError:
            continue
    if not sha:
        raise SourceError(f"revision {rev!r} not found in {url}")
    path = app.path.strip("/") or "."
    out = _git("ls-tree", "-d", "--name-only", sha, path if path != "." else "", cwd=mirror) if path != "." else "."
    if path != "." and not out:
        raise SourceError(f"path {app.path!r} not found at {sha[:8]} in {url}")
    tmp = tempfile.TemporaryDirectory(prefix="spargo-")
    tar = subprocess.run(["git", "archive", "--format=tar", sha] + ([path] if path != "." else []),
                         cwd=mirror, capture_output=True)
    if tar.returncode != 0:
        tmp.cleanup()
        raise SourceError(f"git archive failed: {tar.stderr.decode().strip()}")
    with tarfile.open(fileobj=io.BytesIO(tar.stdout)) as t:
        t.extractall(tmp.name, filter="data")
    return Checkout(os.path.join(tmp.name, path), sha, tmp)
