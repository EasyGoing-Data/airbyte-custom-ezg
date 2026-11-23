FROM python:3.11-slim AS builder
WORKDIR /app
RUN pip install --no-cache-dir poetry
COPY pyproject.toml poetry.lock* ./
COPY source_apple_store_custom ./source_apple_store_custom
RUN poetry config virtualenvs.create false \
    && poetry install --only main --no-root \
    && pip install --no-cache-dir .

FROM python:3.11-slim
ENV TZ=UTC
WORKDIR /airbyte/integration_code
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY main.py ./
COPY source_apple_store_custom ./source_apple_store_custom
ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
LABEL io.airbyte.name=dataezg/source-apple-store-custom
