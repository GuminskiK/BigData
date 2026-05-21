FROM python:3.11-slim

WORKDIR /app

COPY requirements-producer.txt .
RUN pip install --no-cache-dir -r requirements-producer.txt

COPY producer.py .

CMD ["python", "producer.py"]