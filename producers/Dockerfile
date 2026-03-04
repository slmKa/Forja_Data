FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    google-analytics-data \
    google-auth \
    kafka-python \
    python-dotenv

COPY ga4_producer.py .
COPY forja-service-account.json credentials.json

CMD ["python", "ga4_producer.py"]
