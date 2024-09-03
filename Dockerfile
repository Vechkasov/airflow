FROM apache/airflow:latest-python3.11

USER root
RUN apt-get update \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --upgrade pip && pip install matplotlib==3.9.2
