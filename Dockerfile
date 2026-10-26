# One image for the simulator, the streaming job and the batch jobs.
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

# The JRE path differs between amd64 and arm64 (Apple Silicon), so link
# whichever one was installed to a fixed location.
RUN ln -sfn "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/default-java

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    TZ=UTC \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline/ pipeline/
COPY jobs/ jobs/
COPY simulator/ simulator/
COPY scripts/ scripts/
COPY debezium/ debezium/
