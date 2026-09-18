FROM apache/airflow:2.8.1-python3.10

USER root

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       -o Dpkg::Options::="--force-confdef" \
       -o Dpkg::Options::="--force-confold" \
       docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
