"""End-to-end against real containers: emulator (docker runtime) + spargo CLI + git repo."""
import re
import shutil
import subprocess
import threading
import time
import urllib.request

import pytest
import yaml

from conftest import Repo, TOKEN
from spargo import cli
from spargo.client import SnowflakeClient, static_token_auth
from spcs_emulator.runtime import DockerRuntime
from spcs_emulator.server import make_server

pytestmark = pytest.mark.docker
IMAGE = "/APPS/PUBLIC/IMAGES/whoami:latest"


def docker_ok():
    return shutil.which("docker") and subprocess.run(["docker", "info"], capture_output=True).returncode == 0


pytestmark = [pytest.mark.docker, pytest.mark.skipif(not docker_ok(), reason="docker not available")]


def hostnames(url, n=30):
    seen = set()
    for _ in range(n):
        with urllib.request.urlopen(f"http://{url}/", timeout=5) as r:
            seen.add(re.search(r"Hostname: (\S+)", r.read().decode()).group(1))
    return seen


def wait(cond, timeout=60, what=""):
    end = time.time() + timeout
    while time.time() < end:
        v = cond()
        if v:
            return v
        time.sleep(0.3)
    raise AssertionError(f"timed out waiting for {what}")


def svc_doc(replicas, image=IMAGE, env=None):
    c = {"name": "whoami", "image": image, "args": ["--port", "8080"],
         "resources": {"requests": {"cpu": 0.25}}}
    if env:
        c["env"] = env
    return {"apiVersion": "spargo/v1", "kind": "Service", "metadata": {"name": "WHOAMI"},
            "spec": {"computePool": "P", "replicas": replicas,
                     "specification": {"spec": {"containers": [c],
                                                "endpoints": [{"name": "web", "port": 8080, "public": True}]}}}}


def test_gitops_horizontal_scaling_with_real_containers(tmp_path, monkeypatch):
    srv = make_server(runtime=DockerRuntime({IMAGE: "traefik/whoami:latest"}), token=TOKEN, start_delay=0.5, tick=0.3)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{srv.server_address[1]}"
    monkeypatch.setenv("SPARGO_ACCOUNT_URL", url)
    monkeypatch.setenv("SPARGO_TOKEN", TOKEN)
    monkeypatch.setattr(cli, "CACHE", str(tmp_path / "cache"))
    client = SnowflakeClient(url, static_token_auth(TOKEN))
    try:
        repo = Repo(tmp_path / "repo")
        repo.write("pool.yaml", {"apiVersion": "spargo/v1", "kind": "ComputePool", "metadata": {"name": "P"},
                                 "spec": {"instanceFamily": "CPU_X64_S", "minNodes": 1, "maxNodes": 2}})
        repo.write("svc.yaml", svc_doc(3)); repo.commit("3 replicas")
        appf = tmp_path / "app.yaml"
        appf.write_text(yaml.safe_dump({
            "apiVersion": "spargo/v1", "kind": "Application", "metadata": {"name": "e2e"},
            "spec": {"source": {"repoURL": repo.path, "path": "services"},
                     "destination": {"database": "APPS", "schema": "PUBLIC"},
                     "syncPolicy": {"automated": {"prune": True, "selfHeal": True}, "timeoutSeconds": 90}}}))

        assert cli.main(["sync", str(appf)]) == 0
        ep = client.execute("SHOW ENDPOINTS IN SERVICE APPS.PUBLIC.WHOAMI").first()["INGRESS_URL"]
        assert len(hostnames(ep)) == 3, "traffic must be load-balanced over 3 real containers"

        # scale out via git
        repo.write("svc.yaml", svc_doc(5)); repo.commit("5 replicas")
        assert cli.main(["sync", str(appf)]) == 0
        assert len(hostnames(ep, 50)) == 5
        ps = subprocess.run(["docker", "ps", "-q", "--filter", "label=spcs-emulator"], capture_output=True, text=True)
        assert len(ps.stdout.split()) == 5

        # rolling update with a new env var: all 5 containers get replaced
        before = hostnames(ep, 50)
        repo.write("svc.yaml", svc_doc(5, env={"V": "2"})); repo.commit("env change")
        assert cli.main(["sync", str(appf)]) == 0
        after = hostnames(ep, 50)
        assert len(after) == 5 and not (before & after)

        # chaos: kill a container; the platform restarts it; spargo still sees it Healthy and in sync
        victim = next(iter(srv.account.services["APPS.PUBLIC.WHOAMI"].instances.values()))
        subprocess.run(["docker", "rm", "-f", victim.handle.containers[0]], capture_output=True)
        wait(lambda: victim.restarts >= 1 and victim.status == "READY", 60, "restart of killed container")
        assert len(hostnames(ep, 50)) == 5

        # scale in
        repo.write("svc.yaml", svc_doc(2)); repo.commit("2 replicas")
        assert cli.main(["sync", str(appf)]) == 0
        wait(lambda: len(subprocess.run(["docker", "ps", "-q", "--filter", "label=spcs-emulator"],
                                        capture_output=True, text=True).stdout.split()) == 2, 30, "scale-in")
        assert len(hostnames(ep, 30)) == 2

        # prune
        repo.remove("svc.yaml"); repo.commit("remove")
        assert cli.main(["sync", str(appf)]) == 0
        wait(lambda: not subprocess.run(["docker", "ps", "-q", "--filter", "label=spcs-emulator"],
                                        capture_output=True, text=True).stdout.strip(), 30, "containers removed")
    finally:
        srv.shutdown()
        srv.account.shutdown()
