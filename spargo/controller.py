"""Long-running controller: poll git, auto-sync, and self-heal drift (like argocd-application-controller)."""
from __future__ import annotations

import signal
import time
from typing import Callable

from .cli import reconcile_once
from .client import SnowflakeClient, SnowflakeError
from .engine import SyncError
from .manifest import Application, ManifestError
from .source import SourceError


class Controller:
    def __init__(self, apps: list[Application], app_dirs: list[str],
                 client_factory: Callable[[], SnowflakeClient], interval: float = 30,
                 log: Callable[[str], None] = print):
        self.apps, self.dirs, self.factory, self.interval, self.log = apps, app_dirs, client_factory, interval, log
        self.stop = False
        self.last_rev: dict[str, str] = {}
        self.failures: dict[str, int] = {}
        self.next_try: dict[str, float] = {}

    def reconcile(self, app: Application, app_dir: str) -> str:
        pol = app.sync_policy
        client = self.factory()
        plan, rev, _, _ = reconcile_once(app, client, app_dir, apply=False, wait=False)
        new_commit = self.last_rev.get(app.name) != rev
        if plan.in_sync:
            self.last_rev[app.name] = rev
            if plan.relabel and pol.automated:
                reconcile_once(app, client, app_dir, apply=True, wait=False)
            return "Synced"
        # Out of sync: either a new commit (needs auto-sync) or drift on an unchanged commit (self-heal).
        if not pol.automated:
            return "OutOfSync (manual sync required)"
        drift_only = not new_commit
        if drift_only and not pol.self_heal:
            return "OutOfSync (drift; selfHeal disabled)"
        if plan.conflicts:
            return "OutOfSync (conflict: " + "; ".join(str(c) for c in plan.conflicts) + ")"
        self.log(f"{app.name}: {'self-healing drift' if drift_only else 'new revision ' + rev[:12]}: "
                 + " | ".join(str(c) for c in plan.changes))
        reconcile_once(app, client, app_dir, apply=True)
        self.last_rev[app.name] = rev
        return "Synced (auto)"

    def run(self, once: bool = False) -> int:
        for s in (signal.SIGINT, signal.SIGTERM):
            signal.signal(s, lambda *_: setattr(self, "stop", True))
        rc = 0
        while not self.stop:
            for app, d in zip(self.apps, self.dirs):
                if time.time() < self.next_try.get(app.name, 0):
                    continue
                try:
                    res = self.reconcile(app, d)
                    self.failures[app.name] = 0
                    self.log(f"{app.name}: {res}")
                except Exception as e:  # one broken app must not take down the controller
                    n = self.failures[app.name] = self.failures.get(app.name, 0) + 1
                    rc = 1
                    self.next_try[app.name] = time.time() + min(self.interval * 2 ** (n - 1), 600)
                    self.log(f"{app.name}: ERROR (attempt {n}): {e}")
            if once:
                return rc
            end = time.time() + self.interval
            while not self.stop and time.time() < end:
                time.sleep(0.2)
        return 0
