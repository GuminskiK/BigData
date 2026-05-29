FROM apache/spark:3.5.0

USER root

COPY requirements-processor.txt .
RUN pip install --no-cache-dir -r requirements-processor.txt

RUN echo "spark:x:1001:0:Spark User:/app:/bin/bash" >> /etc/passwd
RUN mkdir -p /app/spark_checkpoints /app/.ivy2 && chmod -R 777 /app

WORKDIR /app
COPY spark_batch_job.py .

ENV HOME=/app

USER 1001