# Runs the spargo controller itself as an SPCS service (like argocd-application-controller in-cluster).
FROM --platform=linux/amd64 python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml ./
COPY spargo ./spargo
COPY spcs_emulator ./spcs_emulator
RUN pip install --no-cache-dir .
# Application manifests are mounted from a stage or baked in under /apps
ENTRYPOINT ["spargo", "watch"]
CMD ["/apps/application.yaml"]
