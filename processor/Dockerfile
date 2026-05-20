FROM apache/spark:3.5.0

USER root
RUN pip install vaderSentiment py4j

# KLUCZOWA POPRAWKA: Dodajemy użytkownika 1001 do systemu
# Dzięki temu Java(UnixLoginModule) otrzyma nazwę "spark" zamiast null
RUN echo "spark:x:1001:0:Spark User:/app:/bin/bash" >> /etc/passwd

# Tworzymy strukturę katalogów z pełnymi uprawnieniami
RUN mkdir -p /app/spark_checkpoints /app/.ivy2 && \
    chmod -R 777 /app

WORKDIR /app
COPY spark_processor.py .

# Ustawiamy zmienną HOME
ENV HOME=/app

USER 1001