FROM python:3.9-slim

ENV TZ=UTC
WORKDIR /airbyte/integration_code

# Pin toan bo version theo pip freeze cua image v1.6.0 (build tai tao 100%)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py setup.py ./
COPY source_applovin_max ./source_applovin_max

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.name=dataezg/source-applovin-max
LABEL io.airbyte.version=1.7.0
