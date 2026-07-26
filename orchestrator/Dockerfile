FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

RUN chmod +x /app/deploybench && ln -sf /app/deploybench /usr/local/bin/deploybench

ENV PATH_DATASETS=/data/datasets
ENV PATH_OUTPUT=/data/output
ENV PATH_PIPELINES=/data/pipelines
ENV PATH_CONFIG_SYSTEM=/app/config/system.yaml
ENV ORCHESTRATOR_PORT=58080
ENV PYTHONUNBUFFERED=1

EXPOSE 58080

CMD ["deploybench", "serve"]
