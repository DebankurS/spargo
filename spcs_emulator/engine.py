"""Emulated Snowflake account: compute pools, services, instances, and the reconciler that runs them."""
from __future__ import annotations

import json
import math
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import yaml

from .runtime import DockerRuntime, FakeRuntime, Handle, RoundRobinProxy, parse_cpu, probe_ready
from .sql import (NAME, SqlError, exists, extract_dollar_blocks, not_found, parse_props, split_name)

FAMILY_CPU = {"CPU_X64_XS": 1, "CPU_X64_S": 3, "CPU_X64_M": 6, "CPU_X64_SL": 14, "CPU_X64_L": 28,
              "HIGHMEM_X64_S": 6, "HIGHMEM_X64_M": 28, "HIGHMEM_X64_L": 124,
              "GPU_NV_S": 6, "GPU_NV_M": 44, "GPU_NV_L": 92}


def now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " +0000"


@dataclass
class Instance:
    id: int
    spec_version: int
    status: str = "PENDING"
    handle: Handle | None = None
    restarts: int = 0
    retry_at: float = 0.0
    message: str = ""
    created: str = field(default_factory=now)
    ready_since: float = 0.0


@dataclass
class Pool:
    name: str
    family: str
    min_nodes: int
    max_nodes: int
    auto_resume: bool = True
    auto_suspend_secs: int = 3600
    comment: str = ""
    suspended: bool = False
    idle_since: float = field(default_factory=time.time)
    created: str = field(default_factory=now)
    active_nodes: int = 0


@dataclass
class Service:
    db: str
    schema: str
    name: str
    pool: str
    spec_text: str
    spec: dict
    min_instances: int = 1
    max_instances: int = 1
    min_ready: int | None = None
    auto_resume: bool = True
    eai: list[str] = field(default_factory=list)
    warehouse: str | None = None
    comment: str = ""
    suspended: bool = False
    spec_version: int = 1
    target: int = 1
    load: float | None = None
    last_scale: float = 0.0
    instances: dict[int, Instance] = field(default_factory=dict)
    proxies: dict[int, RoundRobinProxy] = field(default_factory=dict)
    created: str = field(default_factory=now)
    updated: str = field(default_factory=now)
    deleted: bool = False

    @property
    def fqn(self) -> str:
        return f"{self.db}.{self.schema}.{self.name}"


def validate_spec(text: str) -> dict:
    try:
        spec = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise SqlError(f"Invalid service specification: {e}", "394209", "22000")
    if not isinstance(spec, dict) or not isinstance(spec.get("spec"), dict):
        raise SqlError("Invalid service specification: missing top-level 'spec'.", "394209", "22000")
    cs = spec["spec"].get("containers")
    if not isinstance(cs, list) or not cs:
        raise SqlError("Invalid service specification: 'containers' must be a non-empty list.", "394209", "22000")
    for c in cs:
        if not isinstance(c, dict) or not c.get("name") or not c.get("image"):
            raise SqlError("Invalid service specification: each container needs 'name' and 'image'.", "394209", "22000")
    return spec


class Account:
    def __init__(self, runtime=None, start_delay: float = 0.0, compat_minutes: float = 0):
        self.rt = runtime or FakeRuntime()
        self.lock = threading.RLock()
        self.pools: dict[str, Pool] = {}
        self.services: dict[str, Service] = {}
        self.garbage: list[Handle] = []
        self.start_delay = start_delay
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.log_lines: list[str] = []

    # ------------------------------------------------------------ lifecycle
    def start(self, interval: float = 0.5):
        def loop():
            while not self._stop.wait(interval):
                try:
                    self.tick()
                except Exception as e:  # keep the emulator alive
                    self.log_lines.append(f"tick error: {e!r}")
        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()

    def shutdown(self):
        self._stop.set()
        with self.lock:
            for s in self.services.values():
                self._teardown(s)
        for h in self.garbage:
            self.rt.stop(h)
        if hasattr(self.rt, "cleanup_all"):
            self.rt.cleanup_all()

    def _teardown(self, s: Service):
        s.deleted = True
        for i in s.instances.values():
            if i.handle:
                self.garbage.append(i.handle)
        s.instances.clear()
        for p in s.proxies.values():
            p.close()
        s.proxies.clear()

    # ------------------------------------------------------------ SQL
    def execute(self, sql: str, db: str | None = None, schema: str | None = None):
        """Returns (columns, rows) with rows already stringified like the real SQL API."""
        text, blocks = extract_dollar_blocks(sql)
        text = text.strip().rstrip(";").strip()
        if not text:
            raise SqlError("Empty SQL statement.", "000900", "42000")
        with self.lock:
            for pat, fn in self._routes():
                m = re.match(pat, text, re.I | re.S)
                if m:
                    return fn(m, blocks, db, schema)
        raise SqlError(f"SQL compilation error:\nsyntax error line 1 at position 0 unexpected '{text.split()[0]}'.")

    def _routes(self):
        n = NAME
        return [
            (rf"CREATE\s+COMPUTE\s+POOL\s+(IF\s+NOT\s+EXISTS\s+)?{n}\s*(.*)$", self.create_pool),
            (rf"ALTER\s+COMPUTE\s+POOL\s+(IF\s+EXISTS\s+)?{n}\s+(SET\s+.*|SUSPEND|RESUME)$", self.alter_pool),
            (rf"DROP\s+COMPUTE\s+POOL\s+(IF\s+EXISTS\s+)?{n}$", self.drop_pool),
            (r"SHOW\s+COMPUTE\s+POOLS(?:\s+LIKE\s+'([^']*)')?$", self.show_pools),
            (rf"DESC(?:RIBE)?\s+COMPUTE\s+POOL\s+{n}$", self.show_pool_one),
            (rf"CREATE\s+SERVICE\s+(IF\s+NOT\s+EXISTS\s+)?{n}\s+IN\s+COMPUTE\s+POOL\s+{n}\s+(?:FROM\s+SPECIFICATION\s+(__SPEC\d+__))\s*(.*)$", self.create_service),
            (rf"ALTER\s+SERVICE\s+(IF\s+EXISTS\s+)?{n}\s+FROM\s+SPECIFICATION\s+(__SPEC\d+__)$", self.alter_service_spec),
            (rf"ALTER\s+SERVICE\s+(IF\s+EXISTS\s+)?{n}\s+(SET\s+.*|UNSET\s+.*|SUSPEND|RESUME)$", self.alter_service),
            (rf"DROP\s+SERVICE\s+(IF\s+EXISTS\s+)?{n}$", self.drop_service),
            (r"SHOW\s+SERVICES(?:\s+LIKE\s+'([^']*)')?(?:\s+IN\s+(?:SCHEMA\s+)?([\w$.\"]+))?$", self.show_services),
            (rf"DESC(?:RIBE)?\s+SERVICE\s+{n}$", self.describe_service),
            (rf"SHOW\s+SERVICE\s+INSTANCES\s+IN\s+SERVICE\s+{n}$", self.show_instances),
            (rf"SHOW\s+ENDPOINTS\s+IN\s+SERVICE\s+{n}$", self.show_endpoints),
            (r"SELECT\s+SYSTEM\$GET_SERVICE_STATUS\(\s*'([^']+)'\s*\)$", self.svc_status),
            (r"SELECT\s+SYSTEM\$GET_SERVICE_LOGS\(\s*'([^']+)'\s*,\s*'?(\d+)'?\s*,\s*'([^']+)'(?:\s*,\s*(\d+))?\s*\)$", self.svc_logs),
            (r"SELECT\s+CURRENT_VERSION\(\)$", lambda *_: (["CURRENT_VERSION()"], [["8.0.0-spcs-emulator"]])),
        ]

    @staticmethod
    def _like(pattern: str | None, name: str) -> bool:
        if not pattern:
            return True
        rx = "^" + re.escape(pattern).replace("%", ".*").replace("_", ".") + "$"
        return re.match(rx, name, re.I) is not None

    @staticmethod
    def _b(v) -> str:
        return "true" if v else "false"

    # ---- compute pools
    def _pool_view(self, p: Pool) -> dict:
        nsvc = sum(1 for s in self.services.values() if s.pool == p.name)
        state = "SUSPENDED" if p.suspended else ("ACTIVE" if p.active_nodes and any(
            s.instances for s in self.services.values() if s.pool == p.name) else "IDLE")
        return {"name": p.name, "state": state, "min_nodes": p.min_nodes, "max_nodes": p.max_nodes,
                "instance_family": p.family, "num_services": nsvc, "num_jobs": 0,
                "auto_suspend_secs": p.auto_suspend_secs, "auto_resume": self._b(p.auto_resume),
                "active_nodes": p.active_nodes, "idle_nodes": 0 if p.suspended else max(0, p.min_nodes - p.active_nodes),
                "target_nodes": max(p.min_nodes, p.active_nodes), "created_on": p.created,
                "owner": "SPARGO_ROLE", "comment": p.comment, "is_exclusive": "false"}

    def _table(self, rows: list[dict], cols: list[str]):
        return [c.upper() for c in cols], [[None if r.get(c) is None else str(r.get(c)) for c in cols] for r in rows]

    POOL_COLS = ["name", "state", "min_nodes", "max_nodes", "instance_family", "num_services", "num_jobs",
                 "auto_suspend_secs", "auto_resume", "active_nodes", "idle_nodes", "target_nodes",
                 "created_on", "owner", "comment", "is_exclusive"]

    def _apply_pool_props(self, p: Pool, props: dict):
        for k, v in props.items():
            if k == "MIN_NODES": p.min_nodes = int(v)
            elif k == "MAX_NODES": p.max_nodes = int(v)
            elif k == "INSTANCE_FAMILY": p.family = str(v).upper()
            elif k == "AUTO_RESUME": p.auto_resume = bool(v)
            elif k == "AUTO_SUSPEND_SECS": p.auto_suspend_secs = int(v)
            elif k == "COMMENT": p.comment = str(v)
            else:
                raise SqlError(f"SQL compilation error:\ninvalid property '{k}'.")
        if p.family not in FAMILY_CPU:
            raise SqlError(f"SQL compilation error:\ninvalid value '{p.family}' for INSTANCE_FAMILY.")
        if p.min_nodes < 1 or p.max_nodes < p.min_nodes:
            raise SqlError("SQL compilation error:\nMAX_NODES must be >= MIN_NODES and MIN_NODES >= 1.")

    def create_pool(self, m, blocks, db, schema):
        ine, name, rest = m.group(1), split_name(m.group(2), "X", "X")[2], m.group(3)
        if name in self.pools:
            if ine:
                return ["status"], [[f"Pool {name} already exists, statement succeeded."]]
            raise exists("Compute pool", name)
        props = parse_props(rest)
        if "INSTANCE_FAMILY" not in props:
            raise SqlError("SQL compilation error:\nmissing required property INSTANCE_FAMILY.")
        p = Pool(name, str(props["INSTANCE_FAMILY"]).upper(), int(props.get("MIN_NODES", 1)),
                 int(props.get("MAX_NODES", props.get("MIN_NODES", 1))))
        self._apply_pool_props(p, {k: v for k, v in props.items() if k not in ("MIN_NODES", "MAX_NODES", "INSTANCE_FAMILY")})
        self.pools[name] = p
        return ["status"], [[f"Compute Pool {name} successfully created."]]

    def alter_pool(self, m, blocks, db, schema):
        ie, name, act = m.group(1), split_name(m.group(2), "X", "X")[2], m.group(3).strip()
        p = self.pools.get(name)
        if not p:
            if ie:
                return ["status"], [["Statement executed successfully."]]
            raise not_found("Compute pool", name)
        if act.upper() == "SUSPEND":
            p.suspended = True
        elif act.upper() == "RESUME":
            p.suspended = False
        else:
            props = parse_props(act[3:])
            if "INSTANCE_FAMILY" in props:
                raise SqlError("SQL compilation error:\nINSTANCE_FAMILY cannot be altered.")
            self._apply_pool_props(p, props)
        return ["status"], [["Statement executed successfully."]]

    def drop_pool(self, m, blocks, db, schema):
        name = split_name(m.group(2), "X", "X")[2]
        if name not in self.pools:
            if m.group(1):
                return ["status"], [["Drop statement executed successfully (pool does not exist)."]]
            raise not_found("Compute pool", name)
        if any(s.pool == name for s in self.services.values()):
            raise SqlError(f"Cannot drop compute pool {name}: it still has services.", "093147", "0A000")
        del self.pools[name]
        return ["status"], [[f"{name} successfully dropped."]]

    def show_pools(self, m, blocks, db, schema):
        return self._table([self._pool_view(p) for p in self.pools.values() if self._like(m.group(1), p.name)],
                           self.POOL_COLS)

    def show_pool_one(self, m, blocks, db, schema):
        name = split_name(m.group(1), "X", "X")[2]
        if name not in self.pools:
            raise not_found("Compute pool", name)
        return self._table([self._pool_view(self.pools[name])], self.POOL_COLS)

    # ---- services
    def _get(self, raw: str, db, schema, if_exists=False) -> tuple[Service | None, str]:
        d, sc, n = split_name(raw, db, schema)
        fq = f"{d}.{sc}.{n}"
        s = self.services.get(fq)
        if not s and not if_exists:
            raise not_found("Service", fq)
        return s, fq

    def _apply_service_props(self, s: Service, props: dict):
        for k, v in props.items():
            if k == "MIN_INSTANCES": s.min_instances = int(v)
            elif k == "MAX_INSTANCES": s.max_instances = int(v)
            elif k == "MIN_READY_INSTANCES": s.min_ready = int(v)
            elif k == "AUTO_RESUME": s.auto_resume = bool(v)
            elif k == "EXTERNAL_ACCESS_INTEGRATIONS": s.eai = list(v)
            elif k == "QUERY_WAREHOUSE": s.warehouse = str(v).upper()
            elif k == "COMMENT": s.comment = str(v)
            else:
                raise SqlError(f"SQL compilation error:\ninvalid property '{k}'.")
        if "MIN_INSTANCES" in props and "MAX_INSTANCES" not in props:
            pass  # real Snowflake keeps MAX as-is; validated below
        if s.min_instances < 1 or s.max_instances < s.min_instances:
            raise SqlError("SQL compilation error:\nMAX_INSTANCES must be >= MIN_INSTANCES and MIN_INSTANCES >= 1.")
        if s.min_ready is not None and s.min_ready > s.min_instances:
            raise SqlError("SQL compilation error:\nMIN_READY_INSTANCES cannot exceed MIN_INSTANCES.")
        s.target = max(s.min_instances, min(s.target, s.max_instances))
        s.updated = now()

    def create_service(self, m, blocks, db, schema):
        ine, raw, pool_raw, spec_ph, rest = m.groups()
        d, sc, n = split_name(raw, db, schema)
        fq = f"{d}.{sc}.{n}"
        pool = split_name(pool_raw, "X", "X")[2]
        if fq in self.services:
            if ine:
                return ["status"], [[f"Service {n} already exists, statement succeeded."]]
            raise exists("Service", fq)
        if pool not in self.pools:
            raise not_found("Compute pool", pool)
        text = blocks[int(re.search(r"\d+", spec_ph).group())]
        spec = validate_spec(text)
        props = parse_props(rest)
        p = self.pools[pool]
        if p.suspended:
            if not p.auto_resume:
                raise SqlError(f"Compute pool {pool} is suspended and AUTO_RESUME is false.", "093148", "0A000")
            p.suspended = False
        s = Service(d, sc, n, pool, text, spec, min_instances=int(props.get("MIN_INSTANCES", 1)),
                    max_instances=int(props.get("MAX_INSTANCES", props.get("MIN_INSTANCES", 1))))
        self._apply_service_props(s, {k: v for k, v in props.items() if k not in ("MIN_INSTANCES", "MAX_INSTANCES")})
        s.target = s.min_instances
        self.services[fq] = s
        return ["status"], [[f"Service {n} successfully created."]]

    def alter_service_spec(self, m, blocks, db, schema):
        s, fq = self._get(m.group(2), db, schema, bool(m.group(1)))
        if s:
            text = blocks[int(re.search(r"\d+", m.group(3)).group())]
            spec = validate_spec(text)
            if spec != s.spec:
                s.spec, s.spec_text, s.spec_version = spec, text, s.spec_version + 1
            s.updated = now()
        return ["status"], [["Statement executed successfully."]]

    def alter_service(self, m, blocks, db, schema):
        s, fq = self._get(m.group(2), db, schema, bool(m.group(1)))
        act = m.group(3).strip()
        if s:
            if act.upper() == "SUSPEND":
                s.suspended = True
            elif act.upper() == "RESUME":
                s.suspended = False
                self.pools[s.pool].suspended = False
            elif act.upper().startswith("UNSET"):
                for k in re.split(r"[\s,]+", act[5:].strip()):
                    k = k.upper()
                    if k == "QUERY_WAREHOUSE": s.warehouse = None
                    elif k == "EXTERNAL_ACCESS_INTEGRATIONS": s.eai = []
                    elif k == "COMMENT": s.comment = ""
                    elif k: raise SqlError(f"SQL compilation error:\ncannot UNSET '{k}'.")
                s.updated = now()
            else:
                self._apply_service_props(s, parse_props(act[3:]))
        return ["status"], [["Statement executed successfully."]]

    def drop_service(self, m, blocks, db, schema):
        s, fq = self._get(m.group(2), db, schema, bool(m.group(1)))
        if s:
            self._teardown(s)
            del self.services[fq]
            return ["status"], [[f"{s.name} successfully dropped."]]
        return ["status"], [["Drop statement executed successfully (service does not exist)."]]

    def _status(self, s: Service) -> str:
        if s.suspended:
            return "SUSPENDED"
        insts = list(s.instances.values())
        ready = sum(1 for i in insts if i.status == "READY")
        if insts and all(i.status == "FAILED" for i in insts):
            return "FAILED"
        return "READY" if ready >= (s.min_ready or s.min_instances) else "PENDING"

    def _svc_row(self, s: Service) -> dict:
        return {"name": s.name, "status": self._status(s), "database_name": s.db, "schema_name": s.schema,
                "owner": "SPARGO_ROLE", "compute_pool": s.pool, "dns_name": f"{s.name.lower()}.{s.schema.lower()}.{s.db.lower()}.svc.spcs.internal",
                "min_instances": s.min_instances, "max_instances": s.max_instances,
                "min_ready_instances": s.min_ready or s.min_instances, "auto_resume": self._b(s.auto_resume),
                "external_access_integrations": json.dumps(s.eai) if s.eai else None,
                "query_warehouse": s.warehouse, "created_on": s.created, "updated_on": s.updated,
                "comment": s.comment or None, "is_job": "false",
                "is_upgrading": self._b(any(i.spec_version != s.spec_version for i in s.instances.values()) and not s.suspended),
                "current_instances": len(s.instances), "target_instances": s.target}

    SVC_COLS = ["name", "status", "database_name", "schema_name", "owner", "compute_pool", "dns_name",
                "min_instances", "max_instances", "min_ready_instances", "auto_resume",
                "external_access_integrations", "query_warehouse", "created_on", "updated_on", "comment",
                "is_job", "is_upgrading", "current_instances", "target_instances"]

    def show_services(self, m, blocks, db, schema):
        like, scope = m.group(1), m.group(2)
        if scope:
            parts = [p.strip('"').upper() for p in scope.split(".")]
            d, sc = (parts + [None])[:2] if len(parts) == 2 else (db, parts[0])
        else:
            d, sc = db, schema
        rows = [self._svc_row(s) for s in self.services.values()
                if self._like(like, s.name) and (not d or s.db == d) and (not sc or s.schema == sc)]
        return self._table(rows, self.SVC_COLS)

    def describe_service(self, m, blocks, db, schema):
        s, _ = self._get(m.group(1), db, schema)
        row = self._svc_row(s)
        row["spec"] = s.spec_text
        return self._table([row], self.SVC_COLS + ["spec"])

    def show_instances(self, m, blocks, db, schema):
        s, _ = self._get(m.group(1), db, schema)
        rows = [{"database_name": s.db, "schema_name": s.schema, "service_name": s.name, "instance_id": i.id,
                 "status": "SUSPENDED" if s.suspended else i.status, "spec_version": i.spec_version,
                 "restart_count": i.restarts, "message": i.message or None, "creation_time": i.created}
                for i in sorted(s.instances.values(), key=lambda i: i.id)]
        return self._table(rows, ["database_name", "schema_name", "service_name", "instance_id", "status",
                                  "spec_version", "restart_count", "message", "creation_time"])

    def show_endpoints(self, m, blocks, db, schema):
        s, _ = self._get(m.group(1), db, schema)
        rows = []
        for e in s.spec["spec"].get("endpoints") or []:
            port = int(e["port"])
            proxy = s.proxies.get(port)
            url = f"127.0.0.1:{proxy.port}" if proxy else f"{s.name.lower()}.emulator.invalid:{port}"
            rows.append({"name": e["name"], "port": port, "protocol": str(e.get("protocol", "HTTP")).upper(),
                         "is_public": self._b(e.get("public", False)),
                         "ingress_url": url if e.get("public") else None})
        return self._table(rows, ["name", "port", "protocol", "is_public", "ingress_url"])

    def svc_status(self, m, blocks, db, schema):
        s, _ = self._get(m.group(1), db, schema)
        out = [{"status": "SUSPENDED" if s.suspended else i.status, "message": i.message or "Running",
                "containerName": c["name"], "instanceId": str(i.id), "serviceName": s.name,
                "restartCount": i.restarts}
               for i in sorted(s.instances.values(), key=lambda i: i.id) for c in s.spec["spec"]["containers"]]
        return ["SYSTEM$GET_SERVICE_STATUS"], [[json.dumps(out)]]

    def svc_logs(self, m, blocks, db, schema):
        s, _ = self._get(m.group(1), db, schema)
        inst = s.instances.get(int(m.group(2)))
        text = self.rt.logs(inst.handle, int(m.group(4) or 100)) if inst and inst.handle else ""
        return ["SYSTEM$GET_SERVICE_LOGS"], [[text]]

    # ------------------------------------------------------------ reconciler
    def instance_cpu(self, s: Service) -> float:
        return sum(parse_cpu(((c.get("resources") or {}).get("requests") or {}).get("cpu"))
                   for c in s.spec["spec"]["containers"])

    def tick(self):
        with self.lock:
            garbage, self.garbage = self.garbage, []
            services = list(self.services.values())
        for h in garbage:
            self.rt.stop(h)
        for s in services:
            self._reconcile(s)
        with self.lock:
            self._update_pools()

    def _update_pools(self):
        t = time.time()
        for p in self.pools.values():
            cpu = sum(self.instance_cpu(s) * sum(1 for i in s.instances.values() if i.handle)
                      for s in self.services.values() if s.pool == p.name)
            per_node = FAMILY_CPU[p.family]
            p.active_nodes = 0 if p.suspended else min(p.max_nodes, max(p.min_nodes, math.ceil(cpu / per_node)))
            used = any(s.pool == p.name and s.instances and not s.suspended for s in self.services.values())
            if used:
                p.idle_since = t
            elif p.auto_suspend_secs and not p.suspended and t - p.idle_since > p.auto_suspend_secs:
                p.suspended = True
                p.active_nodes = 0

    def _capacity_left(self, s: Service) -> float:
        p = self.pools[s.pool]
        cap = p.max_nodes * FAMILY_CPU[p.family]
        used = sum(self.instance_cpu(o) * sum(1 for i in o.instances.values() if i.handle)
                   for o in self.services.values() if o.pool == p.name)
        return cap - used

    def _stop_instance(self, s: Service, i: Instance):
        h, i.handle = i.handle, None
        if h:
            self.rt.stop(h)

    def _reconcile(self, s: Service):
        with self.lock:
            if s.deleted:
                return
            pool = self.pools.get(s.pool)
            suspended = s.suspended or pool is None or pool.suspended
            lo, hi = s.min_instances, s.max_instances
        t = time.time()
        if suspended:
            for i in list(s.instances.values()):
                self._stop_instance(s, i)
            with self.lock:
                s.instances.clear()
            return

        with self.lock:   # autoscaling from simulated load
            if s.load is not None and t - s.last_scale > 2:
                if s.load >= 70 and s.target < hi:
                    s.target += 1; s.last_scale = t
                elif s.load <= 30 and s.target > lo:
                    s.target -= 1; s.last_scale = t
            s.target = max(lo, min(s.target, hi))
            for iid in [k for k in s.instances if k >= s.target]:
                i = s.instances.pop(iid)
                if i.handle:
                    self.garbage.append(i.handle)
            for iid in range(s.target):
                s.instances.setdefault(iid, Instance(iid, s.spec_version))
            insts = sorted(s.instances.values(), key=lambda i: i.id)
            spec, ver = s.spec, s.spec_version

        # health of running instances
        for i in insts:
            if i.handle:
                st = self.rt.state(i.handle)
                if st != "running":
                    self._stop_instance(s, i)
                    i.status, i.restarts = "FAILED", i.restarts + 1
                    i.message = "container exited"
                    i.retry_at = t + min(2 ** i.restarts, 30)
                elif i.status == "PENDING" and t - i.handle.started >= self.start_delay:
                    ports = sorted(i.handle.ports.values())
                    probe = next((c.get("readinessProbe") for c in spec["spec"]["containers"] if c.get("readinessProbe")), None)
                    port = (i.handle.ports.get(int(probe["port"])) if probe and probe.get("port") else None) \
                        or (ports[0] if ports and self.rt.supports_network else None)
                    if probe_ready(port, probe) if self.rt.supports_network else True:
                        i.status, i.message = "READY", ""

        # rolling update: replace at most one outdated instance, and only once the previous replacement is READY
        starting = any(i.handle and i.status == "PENDING" for i in insts)
        if not starting:
            for i in insts:
                if i.spec_version != ver and i.handle:
                    self._stop_instance(s, i)
                    i.status, i.spec_version = "PENDING", ver
                    break
        for i in insts:   # outdated instances without a container simply adopt the new spec
            if not i.handle and i.spec_version != ver:
                i.spec_version = ver

        # (re)start missing containers if capacity allows
        for i in insts:
            if i.handle or (i.status == "FAILED" and t < i.retry_at):
                continue
            with self.lock:
                left = self._capacity_left(s)
            need = self.instance_cpu(s)
            if need > left:
                i.status, i.message = "PENDING", "Unschedulable: insufficient CPU in compute pool"
                continue
            try:
                i.handle = self.rt.start(s.fqn, i.id, spec)
                i.status, i.message = "PENDING", ""
            except Exception as e:
                i.status, i.message = "FAILED", f"failed to start: {e}"[:300]
                i.restarts += 1
                i.retry_at = t + min(2 ** i.restarts, 30)
            with self.lock:
                if s.deleted and i.handle:
                    self.rt.stop(i.handle); i.handle = None

        # public endpoints -> round robin proxy over READY instances
        if self.rt.supports_network:
            for e in spec["spec"].get("endpoints") or []:
                port = int(e["port"])
                if e.get("public") and port not in s.proxies:
                    s.proxies[port] = RoundRobinProxy(
                        lambda s=s, port=port: [i.handle.ports[port] for i in list(s.instances.values())
                                                if i.status == "READY" and i.handle and port in i.handle.ports])
