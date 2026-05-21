FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY google_trends_processor.py .

CMD ["python", "google_trends_processor.py"]