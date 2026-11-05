"""Desired-state manifests: Application, Service and ComputePool.

Manifests are plain YAML documents kept in git. They are parsed and validated
here into small immutable dataclasses; nothing in this module talks to Snowflake.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from typing import Any

import yaml

API_VERSION = "spargo/v1"
IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


class ManifestError(ValueError):
    pass


def check_ident(value: Any, what: str) -> str:
    if not isinstance(value, str) or not IDENT_RE.match(value):
        raise ManifestError(f"{what} {value!r} is not a valid unquoted Snowflake identifier")
    return value.upper()


def canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def digest(obj: Any) -> str:
    return hashlib.sha256(canonical(obj).encode()).hexdigest()[:16]


@dataclass(frozen=True)
class ComputePoolDef:
    name: str
    instance_family: str
    min_nodes: int
    max_nodes: int
    auto_resume: bool = True
    auto_suspend_secs: int | None = None
    comment: str = ""

    def fingerprint(self) -> str:
        return digest(self.__dict__)


@dataclass(frozen=True)
class ServiceDef:
    name: str
    compute_pool: str
    min_instances: int
    max_instances: int
    specification: dict
    auto_resume: bool = True
    external_access_integrations: tuple[str, ...] = ()
    query_warehouse: str | None = None
    comment: str = ""
    allow_replace: bool = False

    @property
    def autoscaled(self) -> bool:
        return self.max_instances > self.min_instances

    def spec_text(self) -> str:
        """Deterministic YAML spec sent to Snowflake."""
        return yaml.safe_dump(self.specification, sort_keys=True, default_flow_style=False)

    def fingerprint(self) -> str:
        d = dict(self.__dict__)
        d["external_access_integrations"] = list(d["external_access_integrations"])
        d.pop("allow_replace")
        return digest(d)


@dataclass(frozen=True)
class Destination:
    database: str
    schema: str
    role: str | None = None
    warehouse: str | None = None


@dataclass(frozen=True)
class SyncPolicy:
    automated: bool = False
    prune: bool = False
    self_heal: bool = False
    wait_for_healthy: bool = True
    timeout_seconds: int = 600
    allow_replace: bool = False


@dataclass(frozen=True)
class Application:
    name: str
    repo_url: str
    target_revision: str
    path: str
    destination: Destination
    sync_policy: SyncPolicy = field(default_factory=SyncPolicy)


@dataclass
class DesiredState:
    pools: list[ComputePoolDef] = field(default_factory=list)
    services: list[ServiceDef] = field(default_factory=list)


def _int(v: Any, what: str, minimum: int) -> int:
    if isinstance(v, bool) or not isinstance(v, int) or v < minimum:
        raise ManifestError(f"{what} must be an integer >= {minimum}, got {v!r}")
    return v


def _req(d: dict, key: str, ctx: str) -> Any:
    if key not in d or d[key] in (None, ""):
        raise ManifestError(f"{ctx}: missing required field {key!r}")
    return d[key]


def _meta_name(doc: dict, ctx: str) -> str:
    return check_ident(_req(doc.get("metadata") or {}, "name", ctx + ".metadata"), ctx + " name")


def parse_service(doc: dict, base_dir: str) -> ServiceDef:
    name = _meta_name(doc, "Service")
    ctx = f"Service {name}"
    spec = doc.get("spec") or {}
    pool = check_ident(_req(spec, "computePool", ctx), "computePool")

    if "replicas" in spec and "autoscaling" in spec:
        raise ManifestError(f"{ctx}: set either replicas or autoscaling, not both")
    if "autoscaling" in spec:
        a = spec["autoscaling"] or {}
        mn = _int(a.get("minReplicas"), f"{ctx}: autoscaling.minReplicas", 1)
        mx = _int(a.get("maxReplicas"), f"{ctx}: autoscaling.maxReplicas", 1)
        if mx < mn:
            raise ManifestError(f"{ctx}: maxReplicas ({mx}) < minReplicas ({mn})")
    else:
        mn = mx = _int(spec.get("replicas", 1), f"{ctx}: replicas", 1)

    if ("specification" in spec) == ("specificationFile" in spec):
        raise ManifestError(f"{ctx}: set exactly one of specification / specificationFile")
    if "specification" in spec:
        container_spec = copy.deepcopy(spec["specification"])
    else:
        path = os.path.normpath(os.path.join(base_dir, spec["specificationFile"]))
        if os.path.commonpath([path, os.path.normpath(base_dir)]) != os.path.normpath(base_dir):
            raise ManifestError(f"{ctx}: specificationFile escapes the source path")
        with open(path) as f:
            container_spec = yaml.safe_load(f)
    if not isinstance(container_spec, dict) or "spec" not in container_spec:
        raise ManifestError(f"{ctx}: specification must be a mapping with a top-level 'spec' key")
    containers = (container_spec["spec"] or {}).get("containers")
    if not containers:
        raise ManifestError(f"{ctx}: specification.spec.containers must be non-empty")
    if "$$" in yaml.safe_dump(container_spec):
        raise ManifestError(f"{ctx}: specification must not contain '$$'")

    eai = tuple(spec.get("externalAccessIntegrations") or ())
    for e in eai:
        check_ident(e, "externalAccessIntegration")
    wh = spec.get("queryWarehouse")
    if wh:
        check_ident(wh, "queryWarehouse")
    allow_replace = (doc["metadata"].get("annotations") or {}).get("spargo/allow-replace") == "true"
    return ServiceDef(
        name=name, compute_pool=pool, min_instances=mn, max_instances=mx,
        specification=container_spec, auto_resume=bool(spec.get("autoResume", True)),
        external_access_integrations=eai, query_warehouse=wh.upper() if wh else None,
        comment=str(spec.get("comment", "")), allow_replace=allow_replace,
    )


def parse_pool(doc: dict) -> ComputePoolDef:
    name = _meta_name(doc, "ComputePool")
    ctx = f"ComputePool {name}"
    s = doc.get("spec") or {}
    mn = _int(s.get("minNodes", 1), f"{ctx}: minNodes", 1)
    mx = _int(s.get("maxNodes", mn), f"{ctx}: maxNodes", 1)
    if mx < mn:
        raise ManifestError(f"{ctx}: maxNodes < minNodes")
    asus = s.get("autoSuspendSecs")
    return ComputePoolDef(
        name=name, instance_family=check_ident(_req(s, "instanceFamily", ctx), "instanceFamily"),
        min_nodes=mn, max_nodes=mx, auto_resume=bool(s.get("autoResume", True)),
        auto_suspend_secs=None if asus is None else _int(asus, f"{ctx}: autoSuspendSecs", 0),
        comment=str(s.get("comment", "")),
    )


def load_desired(directory: str) -> DesiredState:
    """Load every *.yaml / *.yml under `directory` (recursively) into a DesiredState."""
    state = DesiredState()
    seen: set[tuple[str, str]] = set()
    for root, dirs, files in os.walk(directory):
        dirs[:] = sorted(d for d in dirs if not d.startswith("."))
        for fn in sorted(files):
            if not fn.endswith((".yaml", ".yml")):
                continue
            path = os.path.join(root, fn)
            with open(path) as f:
                try:
                    docs = list(yaml.safe_load_all(f))
                except yaml.YAMLError as e:
                    raise ManifestError(f"{path}: invalid YAML: {e}") from e
            for doc in docs:
                if not isinstance(doc, dict) or doc.get("apiVersion") != API_VERSION:
                    continue  # not ours (e.g. a plain SPCS spec file)
                kind = doc.get("kind")
                try:
                    if kind == "Service":
                        obj = parse_service(doc, root)
                        state.services.append(obj)
                    elif kind == "ComputePool":
                        obj = parse_pool(doc)
                        state.pools.append(obj)
                    else:
                        continue
                except ManifestError as e:
                    raise ManifestError(f"{path}: {e}") from e
                key = (kind, obj.name)
                if key in seen:
                    raise ManifestError(f"{path}: duplicate {kind} {obj.name}")
                seen.add(key)
    return state


def parse_application(doc: dict) -> Application:
    if doc.get("kind") != "Application" or doc.get("apiVersion") != API_VERSION:
        raise ManifestError(f"expected kind Application with apiVersion {API_VERSION}")
    name = (doc.get("metadata") or {}).get("name")
    if not name or not re.match(r"^[a-z0-9][a-z0-9-]*$", name):
        raise ManifestError(f"Application name {name!r} must match [a-z0-9][a-z0-9-]*")
    s = doc.get("spec") or {}
    src = s.get("source") or {}
    dst = s.get("destination") or {}
    sp = s.get("syncPolicy") or {}
    auto = sp.get("automated")
    return Application(
        name=name,
        repo_url=_req(src, "repoURL", "source"),
        target_revision=str(src.get("targetRevision", "HEAD")),
        path=str(src.get("path", ".")),
        destination=Destination(
            database=check_ident(_req(dst, "database", "destination"), "database"),
            schema=check_ident(_req(dst, "schema", "destination"), "schema"),
            role=check_ident(dst["role"], "role") if dst.get("role") else None,
            warehouse=check_ident(dst["warehouse"], "warehouse") if dst.get("warehouse") else None,
        ),
        sync_policy=SyncPolicy(
            automated=auto is not None and auto is not False,
            prune=bool((auto or {}).get("prune", False)) if isinstance(auto, dict) else False,
            self_heal=bool((auto or {}).get("selfHeal", False)) if isinstance(auto, dict) else False,
            wait_for_healthy=bool(sp.get("waitForHealthy", True)),
            timeout_seconds=_int(sp.get("timeoutSeconds", 600), "timeoutSeconds", 1),
            allow_replace=bool(sp.get("allowReplace", False)),
        ),
    )


def load_application(path: str) -> Application:
    with open(path) as f:
        return parse_application(yaml.safe_load(f))
