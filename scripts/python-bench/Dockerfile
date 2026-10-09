FROM python:3.12-slim AS builder
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /build
COPY requirements.txt .
RUN python -m venv /opt/venv && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

FROM python:3.12-slim
ENV PATH=/opt/venv/bin:$PATH PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
RUN groupadd -g 10001 app && useradd -u 10001 -g 10001 -r -s /usr/sbin/nologin app
COPY --from=builder /opt/venv /opt/venv
WORKDIR /app
COPY --chown=10001:10001 run.py .
USER 10001:10001
# One-shot job: no HEALTHCHECK, no EXPOSE.
ENTRYPOINT ["python", "run.py"]
CMD ["--profile", "normal", "--total", "100"]
