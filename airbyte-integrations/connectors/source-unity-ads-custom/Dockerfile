FROM python:3.9-slim

ENV TZ=UTC
WORKDIR /airbyte/integration_code

# Pin version theo requirements.txt cua source-applovin-max-custom (airbyte-cdk 0.51.1), bo pandas/numpy vi khong can
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py setup.py ./
COPY source_unity_ads ./source_unity_ads

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.name=dataezg/source-unity-ads-custom
LABEL io.airbyte.version=1.0.0
