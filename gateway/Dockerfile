FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    GATEWAY_RUN_DIR=/var/run/gateway

WORKDIR /opt/gateway
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 8000: FastAPI operation page   9000: primary recorder entry
# 9001: backup recorder entry
EXPOSE 8000 9000 9001
CMD ["python", "-m", "app.main"]
