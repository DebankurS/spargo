"""Container runtimes for the emulator: real Docker, or an in-memory fake for fast unit tests."""
from __future__ import annotations

import itertools
import socket
import subprocess
import threading
import time
import urllib.request
from dataclasses import dataclass, field

LABEL = "spcs-emulator"


def parse_cpu(v) -> float:
    if v is None:
        return 0.5
    s = str(v)
    return float(s[:-1]) / 1000 if s.endswith("m") else float(s)


def map_image(image: str, image_map: dict[str, str]) -> str:
    """Translate an SPCS image path (/db/schema/repo/img:tag) into something docker can pull/run."""
    if image in image_map:
        return image_map[image]
    if image.startswith("/") or "snowflakecomputing.com" in image.split("/")[0]:
        return image.rstrip("/").split("/")[-1]  # local tag of the same name
    return image


@dataclass
class Handle:
    containers: list[str]                     # docker container names (first owns the network namespace)
    ports: dict[int, int] = field(default_factory=dict)   # container port -> host port
    started: float = field(default_factory=time.time)


class DockerRuntime:
    supports_network = True

    def __init__(self, image_map: dict[str, str] | None = None):
        self.image_map = image_map or {}
        self._n = itertools.count()
        self.cleanup_all()

    @staticmethod
    def _docker(*args: str, check=True) -> str:
        r = subprocess.run(["docker", *args], capture_output=True, text=True)
        if check and r.returncode != 0:
            raise RuntimeError(f"docker {' '.join(args[:2])}: {r.stderr.strip()}")
        return r.stdout.strip()

    def cleanup_all(self):
        ids = self._docker("ps", "-aq", "--filter", f"label={LABEL}", check=False).split()
        if ids:
            self._docker("rm", "-f", *ids, check=False)

    def start(self, service: str, instance: int, spec: dict) -> Handle:
        s = spec.get("spec", {})
        public_ports = sorted({int(e["port"]) for e in s.get("endpoints", []) or []})
        names, first = [], None
        try:
            for c in s["containers"]:
                name = f"spcsemu-{service.lower().replace('.', '-')}-{instance}-{next(self._n)}"
                args = ["run", "-d", "--name", name, "--label", f"{LABEL}=1",
                        "--label", f"{LABEL}.service={service}"]
                for k, v in (c.get("env") or {}).items():
                    args += ["-e", f"{k}={v}"]
                if first is None:
                    args += [a for p in public_ports for a in ("-p", f"127.0.0.1::{p}")]
                else:
                    args += ["--network", f"container:{first}"]
                cmd = c.get("command") or []
                if cmd:
                    args += ["--entrypoint", str(cmd[0])]
                args.append(map_image(c["image"], self.image_map))
                args += [str(x) for x in cmd[1:]] + [str(x) for x in (c.get("args") or [])]
                self._docker(*args)
                names.append(name)
                first = first or name
            ports = {}
            for p in public_ports:
                out = self._docker("port", names[0], f"{p}/tcp")
                ports[p] = int(out.splitlines()[0].rsplit(":", 1)[1])
            return Handle(names, ports)
        except Exception:
            if names:
                self._docker("rm", "-f", *names, check=False)
            raise

    def state(self, h: Handle) -> str:
        """running | exited | gone"""
        for n in h.containers:
            out = self._docker("inspect", "-f", "{{.State.Status}}", n, check=False)
            if not out:
                return "gone"
            status = out.split(":")[0]
            if status != "running":
                return "exited"
        return "running"

    def stop(self, h: Handle):
        self._docker("rm", "-f", *h.containers, check=False)

    def logs(self, h: Handle, tail: int = 100) -> str:
        r = subprocess.run(["docker", "logs", "--tail", str(tail), h.containers[0]],
                           capture_output=True, text=True)
        return r.stdout + r.stderr


class FakeRuntime:
    """No containers at all. `image` values starting with 'crash' exit immediately (for failure tests)."""
    supports_network = False

    def __init__(self, image_map=None, start_delay: float = 0.0):
        self.start_delay = start_delay
        self._next_port = itertools.count(30000)
        self.crashed: set[str] = set()
        self.started: list[tuple[str, int]] = []
        self._n = itertools.count()

    def start(self, service: str, instance: int, spec: dict) -> Handle:
        self.started.append((service, instance))
        c0 = spec["spec"]["containers"][0]
        name = f"fake-{next(self._n)}"
        if str(c0.get("image", "")).startswith("crash"):
            self.crashed.add(name)
        ports = {int(e["port"]): next(self._next_port) for e in spec["spec"].get("endpoints", []) or []}
        return Handle([name], ports)

    def state(self, h: Handle) -> str:
        return "exited" if h.containers[0] in self.crashed else "running"

    def crash(self, h: Handle):
        self.crashed.add(h.containers[0])

    def stop(self, h: Handle):
        pass

    def logs(self, h: Handle, tail: int = 100) -> str:
        return ""


def probe_ready(host_port: int | None, probe: dict | None) -> bool:
    """TCP connect (or HTTP GET when a readinessProbe path is declared) against the published port."""
    if host_port is None:
        return True
    try:
        if probe and probe.get("path"):
            with urllib.request.urlopen(f"http://127.0.0.1:{host_port}{probe['path']}", timeout=2) as r:
                return 200 <= r.status < 400
        with socket.create_connection(("127.0.0.1", host_port), timeout=1):
            return True
    except (OSError, ValueError):
        return False


class RoundRobinProxy:
    """TCP load balancer standing in for the SPCS public endpoint of a multi-instance service."""

    def __init__(self, backends_fn):
        self.backends_fn = backends_fn           # () -> list[int] host ports of READY instances
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(64)
        self.port = self.sock.getsockname()[1]
        self._rr = itertools.count()
        self._closed = False
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while not self._closed:
            try:
                cli, _ = self.sock.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(cli,), daemon=True).start()

    def _serve(self, cli: socket.socket):
        backends = self.backends_fn()
        if not backends:
            cli.close()
            return
        port = backends[next(self._rr) % len(backends)]
        try:
            up = socket.create_connection(("127.0.0.1", port), timeout=3)
        except OSError:
            cli.close()
            return

        def pipe(a, b):
            try:
                while (d := a.recv(65536)):
                    b.sendall(d)
            except OSError:
                pass
            finally:
                for s in (a, b):
                    try:
                        s.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
        threading.Thread(target=pipe, args=(cli, up), daemon=True).start()
        threading.Thread(target=pipe, args=(up, cli), daemon=True).start()

    def close(self):
        self._closed = True
        self.sock.close()
