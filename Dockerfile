FROM python:3.12-slim

LABEL authors="darius"

# PySpark requires a JVM — install OpenJDK 17 (headless = no GUI, smaller)
# Done BEFORE pip install so this heavy layer is cached independently
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

CMD ["python", "src/imobiliare_scraper.py"]
