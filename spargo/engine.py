"""Reconciliation engine: read live state, compute a plan, apply it, wait for health."""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Callable

import yaml

from .client import SnowflakeClient, SnowflakeError
from .manifest import Application, ComputePoolDef, DesiredState, ServiceDef, digest

MARK_RE = re.compile(r"^spargo:app=(\S+) rev=(\S*) fp=(\w+) sp=(\w+)(?: \| (.*))?$", re.S)


class SyncError(RuntimeError):
    pass


# ------------------------------------------------------------------ sql helpers

def q(s: str) -> str:
    return "'" + s.replace("\\", "\\\\").replace("'", "''") + "'"


def mark(app: str, rev: str, fp: str, sp: str, user_comment: str = "") -> str:
    base = f"spargo:app={app} rev={rev[:12]} fp={fp} sp={sp}"
    return base + (f" | {user_comment}" if user_comment else "")


def parse_mark(comment: str | None) -> dict | None:
    m = MARK_RE.match(comment or "")
    return {"app": m[1], "rev": m[2], "fp": m[3], "sp": m[4], "comment": m[5] or ""} if m else None


def spec_fp(svc: ServiceDef) -> str:
    return digest(svc.specification)


def _bool(v) -> bool:
    return str(v).lower() in ("true", "1", "yes")


def _idents(v: str | None) -> tuple[str, ...]:
    return tuple(sorted(x.upper() for x in re.findall(r"[A-Za-z_][A-Za-z0-9_$]*", v or "")))


def subset(want, have) -> bool:
    """True if every key/element in `want` is present with the same value in `have`."""
    if isinstance(want, dict):
        return isinstance(have, dict) and all(k in have and subset(v, have[k]) for k, v in want.items())
    if isinstance(want, list):
        return isinstance(have, list) and len(want) == len(have) and all(subset(a, b) for a, b in zip(want, have))
    return str(want).lower() == str(have).lower()  # tolerate Snowflake re-typing scalars ("8080" vs 8080, true vs True)


# ------------------------------------------------------------------ plan model

@dataclass
class Change:
    action: str            # create | update | replace | delete | conflict
    kind: str              # Service | ComputePool
    name: str
    reasons: list[str] = field(default_factory=list)
    obj: object = None

    def __str__(self):
        return f"{self.action.upper():8} {self.kind}/{self.name}: {'; '.join(self.reasons)}"


@dataclass
class Plan:
    changes: list[Change] = field(default_factory=list)
    relabel: list[ServiceDef] = field(default_factory=list)  # in sync, but git revision stamp is stale

    @property
    def in_sync(self) -> bool:
        return not self.changes

    @property
    def conflicts(self) -> list[Change]:
        return [c for c in self.changes if c.action == "conflict"]


@dataclass
class Health:
    status: str            # Healthy | Progressing | Degraded | Suspended | Missing
    ready: int = 0
    total: int = 0
    message: str = ""


class Engine:
    def __init__(self, client: SnowflakeClient, app: Application,
                 log: Callable[[str], None] = lambda m: None, poll_interval: float = 1.0):
        self.c, self.app, self.log, self.poll = client, app, log, poll_interval
        self.dest = app.destination
        self.c.database, self.c.schema = self.dest.database, self.dest.schema
        if self.dest.role:
            self.c.role = self.dest.role
        if self.dest.warehouse:
            self.c.warehouse = self.dest.warehouse

    def fqn(self, name: str) -> str:
        return f"{self.dest.database}.{self.dest.schema}.{name}"

    # -------------------------------------------------------------- live state
    def live_services(self) -> dict[str, dict]:
        rows = self.c.execute(f"SHOW SERVICES IN SCHEMA {self.dest.database}.{self.dest.schema}")
        out = {}
        for r in rows:
            if _bool(r.get("IS_JOB")):
                continue
            out[r["NAME"].upper()] = r
        return out

    def live_pools(self) -> dict[str, dict]:
        return {r["NAME"].upper(): r for r in self.c.execute("SHOW COMPUTE POOLS")}

    def live_spec(self, name: str) -> dict | None:
        r = self.c.execute(f"DESCRIBE SERVICE {self.fqn(name)}").first()
        try:
            return yaml.safe_load(r["SPEC"]) if r and r.get("SPEC") else None
        except yaml.YAMLError:
            return None

    # -------------------------------------------------------------- plan
    def plan(self, desired: DesiredState, prune: bool | None = None, adopt: bool = False,
             revision: str = "") -> Plan:
        prune = self.app.sync_policy.prune if prune is None else prune
        plan = Plan()
        pools, services = self.live_pools(), self.live_services()

        for p in desired.pools:
            live = pools.get(p.name)
            if not live:
                plan.changes.append(Change("create", "ComputePool", p.name, ["does not exist"], p))
                continue
            m = parse_mark(live.get("COMMENT"))
            if not m and not adopt:
                plan.changes.append(Change("conflict", "ComputePool", p.name,
                                           ["exists but is not managed by spargo (use --adopt)"], p))
                continue
            if m and m["app"] != self.app.name:
                plan.changes.append(Change("conflict", "ComputePool", p.name,
                                           [f"owned by application {m['app']!r}"], p))
                continue
            why = ["adopting existing compute pool"] if not m else []
            if live["INSTANCE_FAMILY"].upper() != p.instance_family:
                why.append(f"instance_family {live['INSTANCE_FAMILY']} -> {p.instance_family} (immutable)")
            for k, want, key in (("min_nodes", p.min_nodes, "MIN_NODES"), ("max_nodes", p.max_nodes, "MAX_NODES")):
                if str(live.get(key)) != str(want):
                    why.append(f"{k} {live.get(key)} -> {want}")
            if _bool(live.get("AUTO_RESUME")) != p.auto_resume:
                why.append(f"auto_resume -> {p.auto_resume}")
            if p.auto_suspend_secs is not None and str(live.get("AUTO_SUSPEND_SECS")) != str(p.auto_suspend_secs):
                why.append(f"auto_suspend_secs {live.get('AUTO_SUSPEND_SECS')} -> {p.auto_suspend_secs}")
            if why:
                act = "conflict" if "immutable" in " ".join(why) else "update"
                plan.changes.append(Change(act, "ComputePool", p.name, why, p))

        desired_names = set()
        for s in desired.services:
            desired_names.add(s.name)
            live = services.get(s.name)
            if not live:
                plan.changes.append(Change("create", "Service", s.name,
                                           [f"does not exist (replicas {s.min_instances}..{s.max_instances})"], s))
                continue
            m = parse_mark(live.get("COMMENT"))
            if not m and not adopt:
                plan.changes.append(Change("conflict", "Service", s.name,
                                           ["exists but is not managed by spargo (use --adopt)"], s))
                continue
            if m and m["app"] != self.app.name:
                plan.changes.append(Change("conflict", "Service", s.name,
                                           [f"owned by application {m['app']!r}"], s))
                continue
            why, replace = [], False
            if live["COMPUTE_POOL"].upper() != s.compute_pool:
                why.append(f"compute_pool {live['COMPUTE_POOL']} -> {s.compute_pool} (immutable, needs replace)")
                replace = True
            for k, want, key in (("min_instances", s.min_instances, "MIN_INSTANCES"),
                                 ("max_instances", s.max_instances, "MAX_INSTANCES")):
                if str(live.get(key)) != str(want):
                    why.append(f"{k} {live.get(key)} -> {want}")
            if _bool(live.get("AUTO_RESUME")) != s.auto_resume:
                why.append(f"auto_resume {live.get('AUTO_RESUME')} -> {s.auto_resume}")
            if _idents(live.get("EXTERNAL_ACCESS_INTEGRATIONS")) != tuple(sorted(s.external_access_integrations)):
                why.append("external_access_integrations changed")
            if (live.get("QUERY_WAREHOUSE") or "").upper() != (s.query_warehouse or ""):
                why.append("query_warehouse changed")
            if m is None:
                why.append("adopting existing service")
            elif m["sp"] != spec_fp(s):
                why.append("specification changed")
            else:  # unchanged in git; check nobody edited the live spec behind our back
                ls = self.live_spec(s.name)
                if ls is not None and not subset(s.specification, ls):
                    why.append("specification drifted from git (live edit)")
            if why:
                plan.changes.append(Change("replace" if replace else "update", "Service", s.name, why, s))
            elif m and m["rev"] != revision[:12] and revision:
                plan.relabel.append(s)

        # Prune: only services we own that are no longer in git.
        owned = [n for n, l in services.items() if (parse_mark(l.get("COMMENT")) or {}).get("app") == self.app.name]
        if prune and not desired.services and not desired.pools and owned:
            plan.changes.append(Change("conflict", "Application", self.app.name,
                                       [f"no manifests found in git but {len(owned)} are deployed; refusing to prune "
                                        "everything (wrong path/revision?). Delete them manually if intended"]))
            prune = False
        if prune:
            for name, live in services.items():
                m = parse_mark(live.get("COMMENT"))
                if m and m["app"] == self.app.name and name not in desired_names:
                    plan.changes.append(Change("delete", "Service", name, ["no longer in git"], None))
        else:
            for name, live in services.items():
                m = parse_mark(live.get("COMMENT"))
                if m and m["app"] == self.app.name and name not in desired_names:
                    self.log(f"note: Service/{name} is orphaned (not in git); prune is disabled")
        return plan

    # -------------------------------------------------------------- sql generation
    def sql_create_pool(self, p: ComputePoolDef, revision: str) -> str:
        parts = [f"CREATE COMPUTE POOL IF NOT EXISTS {p.name}",
                 f"MIN_NODES = {p.min_nodes}", f"MAX_NODES = {p.max_nodes}",
                 f"INSTANCE_FAMILY = {p.instance_family}",
                 f"AUTO_RESUME = {str(p.auto_resume).upper()}"]
        if p.auto_suspend_secs is not None:
            parts.append(f"AUTO_SUSPEND_SECS = {p.auto_suspend_secs}")
        parts.append(f"COMMENT = {q(mark(self.app.name, revision, p.fingerprint(), '0', p.comment))}")
        return " ".join(parts)

    def sql_alter_pool(self, p: ComputePoolDef, revision: str) -> str:
        sets = [f"MIN_NODES = {p.min_nodes}", f"MAX_NODES = {p.max_nodes}",
                f"AUTO_RESUME = {str(p.auto_resume).upper()}"]
        if p.auto_suspend_secs is not None:
            sets.append(f"AUTO_SUSPEND_SECS = {p.auto_suspend_secs}")
        sets.append(f"COMMENT = {q(mark(self.app.name, revision, p.fingerprint(), '0', p.comment))}")
        return f"ALTER COMPUTE POOL {p.name} SET " + " ".join(sets)

    def _svc_props(self, s: ServiceDef, revision: str) -> list[str]:
        props = [f"MIN_INSTANCES = {s.min_instances}", f"MAX_INSTANCES = {s.max_instances}",
                 f"AUTO_RESUME = {str(s.auto_resume).upper()}"]
        if s.external_access_integrations:
            props.append(f"EXTERNAL_ACCESS_INTEGRATIONS = ({', '.join(s.external_access_integrations)})")
        if s.query_warehouse:
            props.append(f"QUERY_WAREHOUSE = {s.query_warehouse}")
        props.append(f"COMMENT = {q(mark(self.app.name, revision, s.fingerprint(), spec_fp(s), s.comment))}")
        return props

    def sql_create_service(self, s: ServiceDef, revision: str) -> str:
        return (f"CREATE SERVICE {self.fqn(s.name)} IN COMPUTE POOL {s.compute_pool} "
                f"FROM SPECIFICATION $${s.spec_text()}$$ " + " ".join(self._svc_props(s, revision)))

    def sql_alter_service_spec(self, s: ServiceDef) -> str:
        return f"ALTER SERVICE {self.fqn(s.name)} FROM SPECIFICATION $${s.spec_text()}$$"

    def sql_alter_service_props(self, s: ServiceDef, revision: str) -> str:
        props = self._svc_props(s, revision)
        return f"ALTER SERVICE {self.fqn(s.name)} SET " + " ".join(props)

    # -------------------------------------------------------------- apply
    def apply(self, plan: Plan, revision: str, allow_replace: bool = False,
              wait: bool | None = None) -> list[str]:
        """Execute a plan. Returns names of services that were touched (for health waiting)."""
        if plan.conflicts:
            raise SyncError("refusing to sync, conflicts: " + "; ".join(str(c) for c in plan.conflicts))
        allow_replace = allow_replace or self.app.sync_policy.allow_replace
        touched: list[str] = []
        order = {"ComputePool": 0, "Service": 1}
        steps = [c for c in plan.changes if c.action in ("create", "update", "replace")]
        steps.sort(key=lambda c: order[c.kind])
        for c in steps:
            self.log(f"apply {c}")
            if c.kind == "ComputePool":
                self.c.execute(self.sql_create_pool(c.obj, revision) if c.action == "create"
                               else self.sql_alter_pool(c.obj, revision))
                continue
            s: ServiceDef = c.obj
            if c.action == "create":
                self.c.execute(self.sql_create_service(s, revision))
            elif c.action == "replace":
                if not (allow_replace or s.allow_replace):
                    raise SyncError(f"Service/{s.name} needs replacement ({'; '.join(c.reasons)}); "
                                    "re-run with --allow-replace or annotate it with spargo/allow-replace: 'true'")
                if s.compute_pool not in self.live_pools():
                    raise SyncError(f"Service/{s.name}: target compute pool {s.compute_pool} does not exist; "
                                    "not dropping the existing service")
                self.c.execute(f"DROP SERVICE {self.fqn(s.name)}")
                self.c.execute(self.sql_create_service(s, revision))
            else:  # update in place
                if any("specification" in r or "adopting" in r for r in c.reasons):
                    self.c.execute(self.sql_alter_service_spec(s))
                self.c.execute(self.sql_alter_service_props(s, revision))
                if not s.query_warehouse and "query_warehouse changed" in c.reasons:
                    self.c.execute(f"ALTER SERVICE {self.fqn(s.name)} UNSET QUERY_WAREHOUSE")
                if not s.external_access_integrations and "external_access_integrations changed" in c.reasons:
                    self.c.execute(f"ALTER SERVICE {self.fqn(s.name)} UNSET EXTERNAL_ACCESS_INTEGRATIONS")
            touched.append(s.name)

        wait = self.app.sync_policy.wait_for_healthy if wait is None else wait
        if wait and touched:
            self.wait_healthy(touched, [s for s in plan.changes if s.action in ("create", "update", "replace")])
        for s in plan.relabel:   # stamp the new git revision only once everything is healthy
            self.c.execute(f"ALTER SERVICE {self.fqn(s.name)} SET COMMENT = "
                           f"{q(mark(self.app.name, revision, s.fingerprint(), spec_fp(s), s.comment))}")
        for c in plan.changes:
            if c.action == "delete":
                self.log(f"apply {c}")
                self.c.execute(f"DROP SERVICE IF EXISTS {self.fqn(c.name)}")
        return touched

    # -------------------------------------------------------------- health
    def health(self, name: str, want_min: int | None = None, live: dict | None = None) -> Health:
        live = live if live is not None else self.live_services().get(name)
        if not live:
            return Health("Missing", message="service not found")
        status = (live.get("STATUS") or "").upper()
        mn = int(live.get("MIN_INSTANCES") or 1) if want_min is None else want_min
        try:
            insts = list(self.c.execute(f"SHOW SERVICE INSTANCES IN SERVICE {self.fqn(name)}"))
        except SnowflakeError as e:
            return Health("Progressing", message=str(e))
        ready = sum(1 for i in insts if (i.get("STATUS") or "").upper() == "READY")
        failed = [i for i in insts if (i.get("STATUS") or "").upper() == "FAILED"]
        if status in ("SUSPENDED", "SUSPENDING"):
            return Health("Suspended", ready, len(insts), status)
        if status in ("FAILED", "INTERNAL_ERROR") or (failed and ready < mn):
            return Health("Degraded", ready, len(insts), f"{len(failed)} failed instance(s)")
        mx = int(live.get("MAX_INSTANCES") or mn)
        if len(insts) > mx:
            return Health("Progressing", ready, len(insts), f"scaling in ({len(insts)} > max {mx})")
        if _bool(live.get("IS_UPGRADING")):
            return Health("Progressing", ready, len(insts), "rolling update in progress")
        if status == "READY" and ready >= mn:
            return Health("Healthy", ready, len(insts))
        return Health("Progressing", ready, len(insts), status)

    def wait_healthy(self, names: list[str], changes: list[Change] | None = None):
        timeout = self.app.sync_policy.timeout_seconds
        deadline = time.time() + timeout
        want = {c.name: c.obj.min_instances for c in (changes or []) if c.kind == "Service"}
        pending = set(names)
        last = {}
        while pending:
            live = self.live_services()
            for n in sorted(pending):
                h = self.health(n, want.get(n), live.get(n))
                if last.get(n) != (h.status, h.ready, h.total):
                    self.log(f"health Service/{n}: {h.status} ({h.ready}/{want.get(n, '?')} ready"
                             f", {h.total} instances) {h.message}".rstrip())
                    last[n] = (h.status, h.ready, h.total)
                if h.status == "Healthy":
                    pending.discard(n)
                elif h.status == "Degraded":
                    raise SyncError(f"Service/{n} is Degraded: {h.message}")
            if not pending:
                return
            if time.time() > deadline:
                raise SyncError(f"timed out after {timeout}s waiting for: {', '.join(sorted(pending))}")
            time.sleep(self.poll)

    def status(self, desired: DesiredState) -> list[dict]:
        live = self.live_services()
        out = []
        for s in desired.services:
            row = live.get(s.name)
            h = self.health(s.name, live=row) if row else Health("Missing")
            m = parse_mark(row.get("COMMENT")) if row else None
            out.append({"name": s.name, "health": h.status, "ready": h.ready, "instances": h.total,
                        "min": row.get("MIN_INSTANCES") if row else None,
                        "max": row.get("MAX_INSTANCES") if row else None,
                        "pool": row.get("COMPUTE_POOL") if row else None,
                        "revision": (m or {}).get("rev"), "message": h.message})
        return out
