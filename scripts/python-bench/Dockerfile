FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY run.py .
CMD ["python", "run.py", "--profile", "normal", "--total", "100"]
