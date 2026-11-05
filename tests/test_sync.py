import time

import pytest

from conftest import POOL, SERVICE, TOKEN, make_app, svc
from spargo.cli import reconcile_once
from spargo.client import SnowflakeClient, SnowflakeError, static_token_auth
from spargo.controller import Controller
from spargo.engine import SyncError, parse_mark


def sync(app, client, repo, **kw):
    kw.setdefault("poll", 0.05)
    return reconcile_once(app, client, ".", apply=True, **kw)


def diff(app, client, **kw):
    return reconcile_once(app, client, ".", apply=False, **kw)[0]


def live(client, name="WEB"):
    return client.execute("SHOW SERVICES").rows and next(
        (r for r in client.execute("SHOW SERVICES IN SCHEMA DB.SCH") if r["NAME"] == name), None)


def instances(client, name="WEB"):
    return list(client.execute(f"SHOW SERVICE INSTANCES IN SERVICE DB.SCH.{name}"))


def test_initial_sync_creates_pool_and_service_with_replicas(client, repo):
    app = make_app(repo.path)
    plan = diff(app, client)
    assert {(c.action, c.kind) for c in plan.changes} == {("create", "ComputePool"), ("create", "Service")}
    sync(app, client, repo)
    row = live(client)
    assert (row["MIN_INSTANCES"], row["MAX_INSTANCES"], row["STATUS"]) == ("2", "2", "READY")
    assert [i["STATUS"] for i in instances(client)] == ["READY", "READY"]
    m = parse_mark(row["COMMENT"])
    assert m["app"] == "demo" and m["rev"] == repo.sha1[:12]
    assert diff(app, client).in_sync


def test_sync_is_idempotent(client, repo):
    app = make_app(repo.path)
    sync(app, client, repo)
    n = len(client.statements_run)
    plan, *_ = sync(app, client, repo)
    assert plan.in_sync
    assert not any(s.startswith(("CREATE", "ALTER", "DROP")) for s in client.statements_run[n:])


def test_scale_out_and_in_via_git(client, repo):
    app = make_app(repo.path)
    sync(app, client, repo)
    repo.write("web.yaml", svc(replicas=4)); repo.commit("scale out")
    plan = diff(app, client)
    assert [c.action for c in plan.changes] == ["update"]
    assert "min_instances 2 -> 4" in plan.changes[0].reasons
    client.statements_run.clear()
    sync(app, client, repo)
    assert len([i for i in instances(client) if i["STATUS"] == "READY"]) == 4
    assert not any("FROM SPECIFICATION" in s for s in client.statements_run), "scaling must not touch the spec"
    repo.write("web.yaml", svc(replicas=1)); repo.commit("scale in")
    sync(app, client, repo)
    assert len(instances(client)) == 1


def test_autoscaling_range_and_load(client, repo, emu):
    app = make_app(repo.path)
    repo.write("web.yaml", svc(**{"autoscaling": {"minReplicas": 1, "maxReplicas": 3}, "replicas": None})) \
        if False else None
    doc = svc(); doc["spec"].pop("replicas"); doc["spec"]["autoscaling"] = {"minReplicas": 1, "maxReplicas": 3}
    repo.write("web.yaml", doc); repo.commit("autoscale")
    sync(app, client, repo)
    row = live(client)
    assert (row["MIN_INSTANCES"], row["MAX_INSTANCES"]) == ("1", "3")
    assert diff(app, client).in_sync
    s = emu.account.services["DB.SCH.WEB"]
    s.load = 95
    deadline = time.time() + 15
    while time.time() < deadline and len(instances(client)) < 3:
        s.last_scale = 0; time.sleep(0.1)
    assert len(instances(client)) == 3
    assert diff(app, client).in_sync, "scaling within [min,max] is not drift"
    s.load = 5
    deadline = time.time() + 15
    while time.time() < deadline and len(instances(client)) > 1:
        s.last_scale = 0; time.sleep(0.1)
    assert len(instances(client)) == 1


def test_spec_change_rolls_instances(client, repo, emu):
    app = make_app(repo.path)
    sync(app, client, repo)
    doc = svc(); doc["spec"]["specification"]["spec"]["containers"][0]["image"] = "/db/sch/repo/img:2"
    repo.write("web.yaml", doc); repo.commit("new image")
    plan = diff(app, client)
    assert plan.changes[0].reasons == ["specification changed"]
    sync(app, client, repo)
    s = emu.account.services["DB.SCH.WEB"]
    assert s.spec_version == 2 and all(i.spec_version == 2 for i in s.instances.values())
    assert [i["STATUS"] for i in instances(client)] == ["READY", "READY"]
    assert "img:2" in client.execute("DESCRIBE SERVICE DB.SCH.WEB").first()["SPEC"]
    assert diff(app, client).in_sync


def test_drift_detected_and_self_healed(client, repo):
    app = make_app(repo.path)
    sync(app, client, repo)
    client.execute("ALTER SERVICE DB.SCH.WEB SET MIN_INSTANCES = 1 MAX_INSTANCES = 1")
    plan = diff(app, client)
    assert plan.changes and "min_instances 1 -> 2" in plan.changes[0].reasons
    ctl = Controller([app], ["."], lambda: client, log=lambda m: None)
    ctl.last_rev[app.name] = repo.sha1          # unchanged commit => pure drift
    assert ctl.reconcile(app, ".") == "Synced (auto)"
    assert live(client)["MIN_INSTANCES"] == "2"


def test_live_spec_edit_is_drift(client, repo):
    app = make_app(repo.path)
    sync(app, client, repo)
    doc = svc(); doc["spec"]["specification"]["spec"]["containers"][0]["image"] = "evil"
    import yaml
    client.execute(f"ALTER SERVICE DB.SCH.WEB FROM SPECIFICATION $${yaml.safe_dump(doc['spec']['specification'])}$$")
    plan = diff(app, client)
    assert "specification drifted from git (live edit)" in plan.changes[0].reasons
    sync(app, client, repo)
    assert diff(app, client).in_sync


def test_controller_respects_selfheal_off_and_manual_mode(client, repo):
    app = make_app(repo.path, self_heal=False)
    sync(app, client, repo)
    client.execute("ALTER SERVICE DB.SCH.WEB SET MIN_INSTANCES = 1 MAX_INSTANCES = 1")
    ctl = Controller([app], ["."], lambda: client, log=lambda m: None)
    ctl.last_rev[app.name] = repo.sha1
    assert "selfHeal disabled" in ctl.reconcile(app, ".")
    assert live(client)["MIN_INSTANCES"] == "1"
    ctl.last_rev.clear()                         # new commit => auto-sync applies regardless
    assert ctl.reconcile(app, ".") == "Synced (auto)"
    manual = make_app(repo.path, automated=False)
    client.execute("ALTER SERVICE DB.SCH.WEB SET MIN_INSTANCES = 1 MAX_INSTANCES = 1")
    assert "manual sync required" in Controller([manual], ["."], lambda: client).reconcile(manual, ".")


def test_prune_removes_only_owned_services(client, repo):
    app = make_app(repo.path)
    sync(app, client, repo)
    client.execute("CREATE SERVICE DB.SCH.FOREIGN IN COMPUTE POOL POOL FROM SPECIFICATION $$"
                   "spec:\n  containers:\n  - name: c\n    image: x\n$$")
    repo.remove("web.yaml"); repo.commit("remove web")
    plan = diff(app, client)
    assert [(c.action, c.name) for c in plan.changes] == [("delete", "WEB")]
    sync(app, client, repo)
    names = {r["NAME"] for r in client.execute("SHOW SERVICES IN SCHEMA DB.SCH")}
    assert names == {"FOREIGN"}


def test_prune_disabled_keeps_orphans(client, repo):
    app = make_app(repo.path, prune=False)
    sync(app, client, repo)
    repo.remove("web.yaml"); repo.commit("remove")
    assert diff(app, client).in_sync
    assert live(client) is not None


def test_conflict_with_unmanaged_and_adopt(client, repo):
    app = make_app(repo.path)
    client.execute("CREATE COMPUTE POOL POOL MIN_NODES=1 MAX_NODES=2 INSTANCE_FAMILY=CPU_X64_XS")
    client.execute("CREATE SERVICE DB.SCH.WEB IN COMPUTE POOL POOL FROM SPECIFICATION $$"
                   "spec:\n  containers:\n  - name: c\n    image: x\n$$")
    plan = diff(app, client)
    assert {c.action for c in plan.changes} == {"conflict"}
    with pytest.raises(SyncError, match="conflicts"):
        sync(app, client, repo)
    sync(app, client, repo, adopt=True)
    assert parse_mark(live(client)["COMMENT"])["app"] == "demo"
    assert diff(app, client).in_sync


def test_other_apps_services_are_not_touched(client, repo):
    sync(make_app(repo.path, name="one"), client, repo)
    plan = diff(make_app(repo.path, name="two"), client)
    assert all(c.action == "conflict" and "owned by application 'one'" in c.reasons[0] for c in plan.changes)


def test_compute_pool_change_requires_replace(client, repo):
    app = make_app(repo.path)
    sync(app, client, repo)
    p2 = {**POOL, "metadata": {"name": "POOL2"}}
    repo.write("pool2.yaml", p2)
    repo.write("web.yaml", svc(computePool="POOL2")); repo.commit("move pool")
    plan = diff(app, client)
    assert any(c.action == "replace" for c in plan.changes)
    with pytest.raises(SyncError, match="allow-replace"):
        sync(app, client, repo)
    sync(app, client, repo, allow_replace=True)
    assert live(client)["COMPUTE_POOL"] == "POOL2"


def test_rollback_to_previous_revision(client, repo):
    app = make_app(repo.path)
    sync(app, client, repo)
    repo.write("web.yaml", svc(replicas=3)); sha2 = repo.commit("v2")
    sync(app, client, repo)
    assert live(client)["MIN_INSTANCES"] == "3"
    sync(app, client, repo, revision=repo.sha1)
    row = live(client)
    assert row["MIN_INSTANCES"] == "2" and parse_mark(row["COMMENT"])["rev"] == repo.sha1[:12]


def test_git_branch_and_tag_revisions(client, repo):
    repo.git("tag", "v1")
    repo.write("web.yaml", svc(replicas=3)); repo.commit("v2")
    app = make_app(repo.path)
    sync(app, client, repo, revision="v1")
    assert live(client)["MIN_INSTANCES"] == "2"
    sync(app, client, repo, revision="main")
    assert live(client)["MIN_INSTANCES"] == "3"


def test_unschedulable_replicas_time_out_as_progressing(client, repo):
    big = svc(replicas=50)
    big["spec"]["specification"]["spec"]["containers"][0]["resources"] = {"requests": {"cpu": 1}}
    repo.write("web.yaml", big); repo.commit("too big")
    app = make_app(repo.path, timeout_seconds=2)
    with pytest.raises(SyncError, match="timed out"):
        sync(app, client, repo)
    msgs = [i["MESSAGE"] for i in instances(client) if i["STATUS"] == "PENDING"]
    assert any("insufficient CPU" in (m or "") for m in msgs)


def test_crashing_container_marks_service_degraded(client, repo):
    doc = svc(); doc["spec"]["specification"]["spec"]["containers"][0]["image"] = "crash-me"
    repo.write("web.yaml", doc); repo.commit("bad")
    with pytest.raises(SyncError, match="Degraded|timed out"):
        sync(make_app(repo.path, timeout_seconds=6), client, repo)


def test_killed_instance_is_restarted(client, repo, emu):
    app = make_app(repo.path)
    sync(app, client, repo)
    s = emu.account.services["DB.SCH.WEB"]
    emu.account.rt.crash(s.instances[0].handle)
    deadline = time.time() + 10
    while time.time() < deadline:
        i = s.instances[0]
        if i.restarts >= 1 and i.status == "READY":
            break
        time.sleep(0.1)
    assert s.instances[0].restarts == 1 and s.instances[0].status == "READY"


def test_bad_token_rejected(emu):
    c = SnowflakeClient(emu.url, static_token_auth("nope"), retries=0)
    with pytest.raises(SnowflakeError, match="Invalid OAuth"):
        c.execute("SELECT CURRENT_VERSION()")


def test_sql_errors_surface(client):
    with pytest.raises(SnowflakeError, match="does not exist"):
        client.execute("DESCRIBE SERVICE DB.SCH.NOPE")
    with pytest.raises(SnowflakeError, match="MAX_INSTANCES"):
        client.execute("CREATE COMPUTE POOL P MIN_NODES=1 MAX_NODES=1 INSTANCE_FAMILY=CPU_X64_XS")
        client.execute("CREATE SERVICE DB.SCH.X IN COMPUTE POOL P FROM SPECIFICATION $$"
                       "spec:\n  containers:\n  - name: c\n    image: x\n$$ MIN_INSTANCES=3 MAX_INSTANCES=2")


def test_prune_refuses_when_git_has_no_services(client, repo):
    app = make_app(repo.path)
    sync(app, client, repo)
    repo.remove("web.yaml"); repo.remove("pool.yaml")
    (__import__("pathlib").Path(repo.path) / "services" / "README.txt").write_text("empty"); repo.commit("oops empty")
    plan = diff(app, client)
    assert plan.conflicts and "refusing to prune" in plan.conflicts[0].reasons[0]
    with pytest.raises(SyncError):
        sync(app, client, repo)
    assert live(client) is not None


def test_removing_warehouse_and_eai_converges(client, repo):
    app = make_app(repo.path)
    client.execute("CREATE COMPUTE POOL IF NOT EXISTS X MIN_NODES=1 MAX_NODES=1 INSTANCE_FAMILY=CPU_X64_XS")
    repo.write("web.yaml", svc(queryWarehouse="WH", externalAccessIntegrations=["EAI"])); repo.commit("with")
    sync(app, client, repo)
    assert live(client)["QUERY_WAREHOUSE"] == "WH"
    repo.write("web.yaml", svc()); repo.commit("without")
    sync(app, client, repo)
    assert live(client)["QUERY_WAREHOUSE"] is None
    assert diff(app, client).in_sync


def test_replace_aborts_before_drop_if_pool_missing(client, repo):
    app = make_app(repo.path)
    sync(app, client, repo)
    repo.write("web.yaml", svc(computePool="NOPOOL")); repo.commit("bad pool")
    with pytest.raises(SyncError, match="does not exist"):
        sync(app, client, repo, allow_replace=True)
    assert live(client) is not None
