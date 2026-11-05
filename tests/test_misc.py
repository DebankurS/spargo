import os
import threading

import pytest
import yaml

from conftest import SERVICE, TOKEN, svc
from spargo import cli
from spargo.client import SnowflakeClient, SnowflakeError, keypair_auth
from spargo.engine import q
from spargo.manifest import ManifestError, load_desired, parse_application, parse_service
from spcs_emulator.runtime import FakeRuntime
from spcs_emulator.server import make_server


def test_manifest_validation(tmp_path):
    for bad in ({"replicas": 0}, {"replicas": 2, "autoscaling": {"minReplicas": 1, "maxReplicas": 2}},
                {"autoscaling": {"minReplicas": 3, "maxReplicas": 2}}, {"computePool": "bad name"},
                {"specification": {"nope": 1}}):
        with pytest.raises(ManifestError):
            parse_service(svc(**bad), str(tmp_path))
    d = svc(); d["spec"]["specification"]["spec"]["containers"][0]["args"] = ["$$"]
    with pytest.raises(ManifestError, match=r"\$\$"):
        parse_service(d, str(tmp_path))


def test_spec_file_reference_and_path_escape(tmp_path):
    (tmp_path / "s.yaml").write_text(yaml.safe_dump(SERVICE["spec"]["specification"]))
    d = svc(); del d["spec"]["specification"]; d["spec"]["specificationFile"] = "s.yaml"
    assert parse_service(d, str(tmp_path)).specification["spec"]["containers"]
    d["spec"]["specificationFile"] = "../x.yaml"
    with pytest.raises(ManifestError, match="escapes"):
        parse_service(d, str(tmp_path / "sub")) if False else parse_service(d, str(tmp_path))


def test_application_parsing():
    a = parse_application({"apiVersion": "spargo/v1", "kind": "Application", "metadata": {"name": "x"},
                           "spec": {"source": {"repoURL": "r"}, "destination": {"database": "d", "schema": "s"},
                                    "syncPolicy": {"automated": {"prune": True}}}})
    assert a.sync_policy.automated and a.sync_policy.prune and not a.sync_policy.self_heal
    with pytest.raises(ManifestError):
        parse_application({"apiVersion": "spargo/v1", "kind": "Application", "metadata": {"name": "Bad_Name"}, "spec": {}})


def test_sql_quoting():
    assert q("it's a \\ test") == "'it''s a \\\\ test'"


def test_duplicate_names_rejected(tmp_path):
    for n in ("a.yaml", "b.yaml"):
        (tmp_path / n).write_text(yaml.safe_dump(SERVICE))
    with pytest.raises(ManifestError, match="duplicate"):
        load_desired(str(tmp_path))


def test_keypair_jwt_auth(tmp_path):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    key = rsa.generate_private_key(65537, 2048)
    priv = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    pub = key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
    srv = make_server(runtime=FakeRuntime(), public_key_pem=pub, tick=0.1)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{srv.server_address[1]}"
    try:
        c = SnowflakeClient(url, keypair_auth("emu", "spargo", priv))
        assert c.execute("SELECT CURRENT_VERSION()").first()
        other = rsa.generate_private_key(65537, 2048).private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
        with pytest.raises(SnowflakeError):
            SnowflakeClient(url, keypair_auth("emu", "spargo", other), retries=0).execute("SELECT CURRENT_VERSION()")
    finally:
        srv.shutdown(); srv.account.shutdown()


def test_cli_end_to_end(emu, repo, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("SPARGO_ACCOUNT_URL", emu.url)
    monkeypatch.setenv("SPARGO_TOKEN", TOKEN)
    appf = tmp_path / "app.yaml"
    appf.write_text(yaml.safe_dump({
        "apiVersion": "spargo/v1", "kind": "Application", "metadata": {"name": "cli"},
        "spec": {"source": {"repoURL": repo.path, "path": "services"},
                 "destination": {"database": "DB", "schema": "SCH"},
                 "syncPolicy": {"automated": {"prune": True}, "timeoutSeconds": 20}}}))
    assert cli.main(["validate", str(appf)]) == 0
    assert cli.main(["diff", str(appf)]) == 1          # out of sync
    assert cli.main(["sync", str(appf)]) == 0
    assert cli.main(["diff", str(appf)]) == 0          # in sync
    capsys.readouterr()
    assert cli.main(["status", str(appf), "--json"]) == 0
    out = capsys.readouterr().out
    assert '"Synced"' in out and '"Healthy"' in out
    assert cli.main(["watch", str(appf), "--once"]) == 0


def test_cli_reports_errors(emu, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("SPARGO_ACCOUNT_URL", emu.url)
    monkeypatch.setenv("SPARGO_TOKEN", "wrong")
    appf = tmp_path / "app.yaml"
    (tmp_path / "svc").mkdir()
    appf.write_text(yaml.safe_dump({
        "apiVersion": "spargo/v1", "kind": "Application", "metadata": {"name": "e"},
        "spec": {"source": {"repoURL": str(tmp_path), "path": "svc"},
                 "destination": {"database": "DB", "schema": "SCH"}}}))
    assert cli.main(["diff", str(appf)]) == 2
    assert "error:" in capsys.readouterr().err
