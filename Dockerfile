FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libxml2-dev \
    libxslt1-dev \
    libffi-dev \
    curl \
    tini \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/logs
RUN mkdir -p /opt/dagster/dagster_home

COPY dagster.yaml /opt/dagster/dagster_home/dagster.yaml

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV DAGSTER_HOME=/opt/dagster/dagster_home

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/app/orchestration/workspace.yaml"]