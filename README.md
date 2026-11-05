# spargo — GitOps continuous delivery for Snowpark Container Services

An Argo CD equivalent for Snowflake SPCS. Git is the source of truth for your **compute pools** and
**services**; `spargo` diffs git against what is live in Snowflake and reconciles it, including
**horizontal scaling** (`MIN_INSTANCES` / `MAX_INSTANCES`), rolling spec updates, drift detection,
self-healing, pruning, and rollback.

Snowflake runs Kubernetes under SPCS but does not expose it, so spargo talks only to what you *do* have:
SQL (`CREATE/ALTER/SHOW SERVICE`, …) over the [SQL API v2](https://docs.snowflake.com/en/developer-guide/sql-api/index).

> **Status / honesty note.** This was built and tested **without a real Snowflake account**. No SPCS
> emulator exists, so the repo ships one (`spcs-emulator`) that implements the SQL API and the SPCS
> statements spargo uses, and runs replicas as real Docker containers. The SQL and result columns follow
> Snowflake's documentation, but please do a first run against a **non-production** account/schema
> (see [First run on real Snowflake](#first-run-on-real-snowflake)) and report any divergence.

## Concepts (Argo CD ↔ spargo)

| Argo CD | spargo |
|---|---|
| Application | `kind: Application` — repo + revision + path → database/schema |
| Deployment replicas | `spec.replicas: N` → `MIN_INSTANCES = MAX_INSTANCES = N`; or `autoscaling: {minReplicas, maxReplicas}` (Snowflake scales on CPU/memory) |
| Sync / diff / OutOfSync | `spargo sync` / `spargo diff` (exit 1 if out of sync) / `status` |
| Auto-sync, prune, self-heal | `syncPolicy.automated.{prune,selfHeal}` run by `spargo watch` |
| Rollback | `spargo sync app.yaml --revision <sha-or-tag>` |
| Ownership labels | Stored in the object `COMMENT` (`spargo:app=… rev=… fp=… sp=…`); no extra tables needed |

How a sync works: read git → `SHOW SERVICES` / `DESCRIBE SERVICE` / `SHOW COMPUTE POOLS` → plan →
`CREATE COMPUTE POOL`, `CREATE SERVICE`, `ALTER SERVICE … FROM SPECIFICATION` (rolling update),
`ALTER SERVICE … SET MIN_INSTANCES/MAX_INSTANCES/…` (scaling; never touches the spec) → wait until
`SHOW SERVICE INSTANCES` shows enough `READY` instances → prune.

Safety rules: spargo only modifies/deletes objects whose comment says they belong to *that* Application;
pre-existing objects are a **conflict** until you pass `--adopt`; immutable changes (e.g. moving a service to
another compute pool) require `--allow-replace`; compute pools are never dropped.

## Install

```bash
uv sync --extra dev              # creates .venv; needs git, and Docker for the emulator's real containers
source .venv/bin/activate        # or prefix every command below with `uv run`
```

## Try it locally in 2 minutes (no Snowflake needed)

```bash
./examples/demo.sh
```

It starts the emulator, commits a 3-replica service (`traefik/whoami`) to a temp git repo, syncs it, proves
traffic is spread over 3 containers, scales to 5 by editing git, then edits the service by hand and shows
self-heal reverting it. Or do it by hand:

```bash
# terminal 1: emulator (maps the SPCS image path used in the example to a public docker image)
spcs-emulator --port 8085 --token demo \
  --image-map "/APPS/PUBLIC/IMAGES/whoami:latest=traefik/whoami:latest"

# terminal 2
export SPARGO_ACCOUNT_URL=http://127.0.0.1:8085 SPARGO_TOKEN=demo
cp -r examples/whoami /tmp/whoami && cd /tmp/whoami && git init -b main && git add -A && git commit -m init
spargo validate application.yaml          # offline manifest check
spargo diff     application.yaml          # shows creates; exit code 1
spargo sync     application.yaml          # creates pool + service, waits for 3/3 READY
spargo status   application.yaml
curl -s "$(curl -s -H 'Authorization: Bearer demo' localhost:8085/api/v2/statements \
   -d '{"statement":"SHOW ENDPOINTS IN SERVICE APPS.PUBLIC.WHOAMI"}' | python -c 'import json,sys;print(json.load(sys.stdin)["data"][0][4])')"
# edit services/whoami.yaml: replicas: 5, then
git commit -am scale && spargo sync application.yaml
spargo sync application.yaml --revision HEAD~1     # rollback to 3 replicas
spargo watch application.yaml --interval 15        # controller: auto-sync + self-heal + prune
```

Emulator extras: `curl -XPOST localhost:8085/_emu/load -H 'Authorization: Bearer demo' -d '{"service":"APPS.PUBLIC.WHOAMI","cpu":90}'`
simulates CPU load so `autoscaling` services scale between min and max; `GET /_emu/state` dumps everything;
`POST /_emu/kill {"service":…,"instance":0}` kills an instance's container.
Emulator limits: single process, in-memory, no image repositories, stages, roles/RBAC, or networking between services.

## Manifests

`application.yaml` (see `examples/whoami/application.yaml`):

```yaml
apiVersion: spargo/v1
kind: Application
metadata: {name: whoami}
spec:
  source: {repoURL: git@github.com:acme/spcs-config.git, targetRevision: main, path: services}
  destination: {database: APPS, schema: PUBLIC, role: SPARGO_ROLE, warehouse: SPARGO_WH}
  syncPolicy:
    automated: {prune: true, selfHeal: true}
    waitForHealthy: true
    timeoutSeconds: 600
```

`repoURL` may be any git URL (auth via your normal git config / credential helper / `GIT_ASKPASS`), a local git
repo, or a plain directory. Files under `path` (recursive) with `apiVersion: spargo/v1`:

```yaml
kind: ComputePool
metadata: {name: MY_POOL}
spec: {instanceFamily: CPU_X64_S, minNodes: 1, maxNodes: 4, autoSuspendSecs: 600}
---
kind: Service
metadata: {name: API}                       # annotation spargo/allow-replace: "true" permits drop+create
spec:
  computePool: MY_POOL
  replicas: 3                               # or: autoscaling: {minReplicas: 2, maxReplicas: 8}
  externalAccessIntegrations: [MY_EAI]      # optional
  queryWarehouse: MY_WH                     # optional
  specification:                            # or specificationFile: ./api-spec.yaml
    spec:
      containers:
        - {name: api, image: /APPS/PUBLIC/IMAGES/api:1.4.2, resources: {requests: {cpu: 0.5}}}
      endpoints: [{name: http, port: 8080, public: true}]
```

Remember that replicas need node capacity: size `maxNodes` (and the container resource requests) so
`replicas × requests` fits, or the sync waits in `Progressing` and times out.

## First run on real Snowflake

1. Create a role and a scratch schema, and grant what spargo needs:
   ```sql
   CREATE ROLE SPARGO_ROLE;
   GRANT USAGE ON DATABASE APPS TO ROLE SPARGO_ROLE;
   GRANT USAGE, CREATE SERVICE ON SCHEMA APPS.PUBLIC TO ROLE SPARGO_ROLE;
   GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE SPARGO_ROLE;   -- only if spargo manages pools
   GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE SPARGO_ROLE; -- only for public endpoints
   -- existing pools: GRANT USAGE, MONITOR ON COMPUTE POOL <p> TO ROLE SPARGO_ROLE;
   ```
2. Authenticate (pick one):
   ```bash
   export SPARGO_ACCOUNT_URL=https://<org>-<account>.snowflakecomputing.com
   export SPARGO_AUTH=pat   SPARGO_TOKEN=<programmatic access token>       # default
   # or key-pair JWT:
   export SPARGO_AUTH=keypair SPARGO_ACCOUNT=<org>-<account> SPARGO_USER=<user> SPARGO_PRIVATE_KEY_FILE=rsa_key.p8
   # or OAuth: SPARGO_AUTH=oauth SPARGO_TOKEN=<token>
   ```
3. Push your images to an image repository and reference them as `/db/schema/repo/image:tag`.
4. `spargo diff app.yaml` (read-only) → `spargo sync app.yaml`.
5. Optional: run the controller *inside* SPCS (`deploy/Dockerfile`, `deploy/controller-service.yaml`); it
   authenticates with the OAuth token Snowflake mounts at `/snowflake/session/token` automatically.

## Tests

```bash
uv run pytest -q                         # 32 tests: unit + integration vs. emulator + real-Docker end-to-end
uv run pytest -q -m "not docker"         # skip the Docker test
```

## Known limitations

- Verified only against the emulator; see the honesty note above.
- Drift detection compares scaling/settings columns, a spec hash stored in the comment, and whether the git spec is
  a subset of the live spec; a live-only *added* spec key is not flagged.
- Job services, volumes/stages, secrets, grants and image-repository management are not managed.
- Sync is per-Application and serial; the controller polls (no webhooks).
- Prune never drops compute pools, and refuses to run when git contains no manifests at all (typo / wrong path guard).
- Not verifiable without a real account: exact `SHOW SERVICE INSTANCES` status values, `COMMENT` length limits, and whether Snowflake re-types spec scalars (comparison is case/type tolerant to limit false drift).
