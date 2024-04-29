FROM python:3.10-slim as base
COPY requirements.txt .
RUN pip install --no-cache-dir -r ./requirements.txt

FROM base as dagster

RUN mkdir -p /app
RUN mkdir -p dagster/dagster_home
RUN touch dagster/dagster_home/dagster.yaml

COPY . /app

WORKDIR /app

EXPOSE 3000

ENTRYPOINT ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
