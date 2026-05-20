FROM python:3.11-slim

WORKDIR /app
RUN pip install kafka-python

COPY twitch_mock_producer.py .
CMD ["python", "twitch_mock_producer.py"]