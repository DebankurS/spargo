"""spargo command line: diff | sync | status | watch | validate."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

from .client import SnowflakeClient, SnowflakeError, from_env
from .engine import Engine, Plan, SyncError
from .manifest import Application, ManifestError, load_application, load_desired
from .source import SourceError, resolve

CACHE = os.environ.get("SPARGO_CACHE", os.path.expanduser("~/.cache/spargo"))


def log(msg: str):
    print(f"[spargo] {msg}", flush=True)


def render_plan(plan: Plan) -> str:
    if plan.in_sync:
        return "In sync: live state matches git."
    return "\n".join(str(c) for c in plan.changes)


def reconcile_once(app: Application, client: SnowflakeClient, app_dir: str, *, apply: bool,
                   revision: str | None = None, prune: bool | None = None, adopt: bool = False,
                   allow_replace: bool = False, wait: bool | None = None, poll: float = 1.0):
    """Fetch the source, plan, and optionally apply. Returns (plan, revision)."""
    if revision:
        app = Application(**{**app.__dict__, "target_revision": revision})
    co = resolve(app, CACHE, app_dir)
    try:
        desired = load_desired(co.directory)
        eng = Engine(client, app, log=log, poll_interval=poll)
        plan = eng.plan(desired, prune=prune, adopt=adopt, revision=co.revision)
        if apply:
            eng.apply(plan, co.revision, allow_replace=allow_replace, wait=wait)
        return plan, co.revision, eng, desired
    finally:
        co.cleanup()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="spargo", description="GitOps CD for Snowpark Container Services")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p, apply_opts=False):
        p.add_argument("app_file", help="Application manifest (YAML)")
        p.add_argument("--revision", help="git revision to use instead of the manifest's targetRevision")
        p.add_argument("--adopt", action="store_true", help="take over existing unmanaged objects")
        if apply_opts:
            p.add_argument("--prune", action="store_true", default=None, help="delete managed services removed from git")
            p.add_argument("--allow-replace", action="store_true", help="allow drop+create for immutable changes")
            p.add_argument("--no-wait", action="store_true", help="do not wait for services to become healthy")

    common(sub.add_parser("diff", help="show what a sync would change (exit 1 if out of sync)"))
    common(sub.add_parser("sync", help="apply git state to Snowflake"), True)
    p = sub.add_parser("status", help="show health of the application's services")
    p.add_argument("app_file"); p.add_argument("--json", action="store_true")
    p = sub.add_parser("watch", help="run the controller loop (auto-sync / self-heal) for Application files")
    p.add_argument("app_file", nargs="+"); p.add_argument("--interval", type=float, default=30)
    p.add_argument("--once", action="store_true", help="single pass (for cron / CI)")
    p = sub.add_parser("validate", help="validate manifests offline (no Snowflake access)")
    p.add_argument("app_file")
    a = ap.parse_args(argv)

    try:
        app = load_application(a.app_file if a.cmd != "watch" else a.app_file[0])
        app_dir = os.path.dirname(os.path.abspath(a.app_file if a.cmd != "watch" else a.app_file[0]))

        if a.cmd == "validate":
            co = resolve(app, CACHE, app_dir)
            try:
                d = load_desired(co.directory)
            finally:
                co.cleanup()
            log(f"OK @ {co.revision[:12]}: {len(d.pools)} compute pool(s), {len(d.services)} service(s)")
            for s in d.services:
                log(f"  Service/{s.name}: pool={s.compute_pool} replicas={s.min_instances}..{s.max_instances}")
            return 0

        if a.cmd == "watch":
            from .controller import Controller
            return Controller([load_application(f) for f in a.app_file],
                              [os.path.dirname(os.path.abspath(f)) for f in a.app_file],
                              from_env, interval=a.interval, log=log).run(once=a.once)

        client = from_env()
        if a.cmd in ("diff", "sync"):
            apply = a.cmd == "sync"
            plan, rev, _, _ = reconcile_once(
                app, client, app_dir, apply=apply, revision=a.revision, adopt=a.adopt,
                prune=getattr(a, "prune", None), allow_replace=getattr(a, "allow_replace", False),
                wait=False if getattr(a, "no_wait", False) else None)
            log(f"revision {rev[:12]}")
            print(render_plan(plan))
            if apply:
                log("sync complete")
            return 0 if (apply or plan.in_sync) else 1

        if a.cmd == "status":
            co = resolve(app, CACHE, app_dir)
            try:
                desired = load_desired(co.directory)
            finally:
                co.cleanup()
            eng = Engine(client, app)
            rows = eng.status(desired)
            plan = eng.plan(desired, revision=co.revision)
            if a.json:
                print(json.dumps({"sync": "Synced" if plan.in_sync else "OutOfSync", "services": rows}, indent=2))
            else:
                print(f"Application {app.name}: {'Synced' if plan.in_sync else 'OutOfSync'} @ {co.revision[:12]}")
                print(f"{'SERVICE':24}{'HEALTH':13}{'READY':8}{'REPLICAS':10}{'POOL':20}REV")
                for r in rows:
                    print(f"{r['name']:24}{r['health']:13}{str(r['ready']) + '/' + str(r['instances']):8}"
                          f"{str(r['min']) + '..' + str(r['max']):10}{str(r['pool']):20}{r['revision'] or '-'}")
            return 0
    except (ManifestError, SourceError, SnowflakeError, SyncError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
