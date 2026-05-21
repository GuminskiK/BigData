FROM python:3.11-slim

WORKDIR /app

COPY requirements-minio.txt .
RUN pip install --no-cache-dir -r requirements-minio.txt

COPY consumer_to_minio.py .

CMD ["python", "consumer_to_minio.py"]