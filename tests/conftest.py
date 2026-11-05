import copy
import os
import shutil
import subprocess
import threading

import pytest
import yaml

from spargo.client import SnowflakeClient, static_token_auth
from spargo.manifest import Application, Destination, SyncPolicy
from spcs_emulator.runtime import FakeRuntime
from spcs_emulator.server import make_server

TOKEN = "test-token"


@pytest.fixture
def emu():
    srv = make_server(runtime=FakeRuntime(), token=TOKEN, tick=0.05)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    srv.url = f"http://127.0.0.1:{srv.server_address[1]}"
    yield srv
    srv.shutdown()
    srv.account.shutdown()


@pytest.fixture
def client(emu):
    return SnowflakeClient(emu.url, static_token_auth(TOKEN))


def make_app(repo, name="demo", **sp):
    return Application(
        name=name, repo_url=str(repo), target_revision="HEAD", path="services",
        destination=Destination("DB", "SCH"),
        sync_policy=SyncPolicy(**{"automated": True, "prune": True, "self_heal": True,
                                  "timeout_seconds": 20, **sp}))


SERVICE = {
    "apiVersion": "spargo/v1", "kind": "Service", "metadata": {"name": "WEB"},
    "spec": {"computePool": "POOL", "replicas": 2,
             "specification": {"spec": {"containers": [{"name": "c", "image": "/db/sch/repo/img:1"}],
                                        "endpoints": [{"name": "http", "port": 8080, "public": True}]}}},
}
POOL = {"apiVersion": "spargo/v1", "kind": "ComputePool", "metadata": {"name": "POOL"},
        "spec": {"instanceFamily": "CPU_X64_XS", "minNodes": 1, "maxNodes": 2}}


class Repo:
    """A throwaway git repo holding manifests."""

    def __init__(self, path):
        self.path = str(path)
        os.makedirs(os.path.join(self.path, "services"))
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.email", "t@t")
        self.git("config", "user.name", "t")

    def git(self, *a):
        return subprocess.run(["git", *a], cwd=self.path, check=True, capture_output=True, text=True).stdout.strip()

    def write(self, fname, doc):
        with open(os.path.join(self.path, "services", fname), "w") as f:
            yaml.safe_dump(doc, f)

    def commit(self, msg="c"):
        self.git("add", "-A")
        self.git("commit", "-q", "-m", msg)
        return self.git("rev-parse", "HEAD")

    def remove(self, fname):
        os.remove(os.path.join(self.path, "services", fname))


@pytest.fixture
def repo(tmp_path):
    r = Repo(tmp_path / "repo")
    r.write("pool.yaml", POOL)
    r.write("web.yaml", SERVICE)
    r.sha1 = r.commit("initial")
    return r


@pytest.fixture(autouse=True)
def cache(tmp_path, monkeypatch):
    import spargo.cli as cli
    monkeypatch.setattr(cli, "CACHE", str(tmp_path / "cache"))


def svc(**over):
    d = copy.deepcopy(SERVICE)
    d["spec"].update(over)
    return d
