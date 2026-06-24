FROM python:3.11-slim

WORKDIR /airbyte/integration_code

# Install the connector (pyproject uses poetry-core build backend, so `pip install .` works)
COPY pyproject.toml README.md ./
COPY source_google_play_console ./source_google_play_console
COPY main.py ./
RUN pip install --no-cache-dir .

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.version=0.1.0
LABEL io.airbyte.name=<DOCKERHUB_USER>/source-google-play-console

# Build & push (run from this folder, target VM is linux/amd64):
#   docker buildx build --platform linux/amd64 \
#     -t <DOCKERHUB_USER>/source-google-play-console:0.1.0 . --push
