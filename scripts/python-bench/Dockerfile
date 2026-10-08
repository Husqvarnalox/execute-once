FROM python:3.11-slim
RUN groupadd -g 10001 app && useradd -u 10001 -g 10001 -r -s /usr/sbin/nologin app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY --chown=10001:10001 run.py .
USER 10001:10001
CMD ["python", "run.py", "--profile", "normal", "--total", "100"]
