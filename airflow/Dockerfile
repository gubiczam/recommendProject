FROM apache/airflow:2.10.5

USER root
COPY requirements.txt /tmp/requirements.txt
USER airflow

RUN pip install --no-cache-dir -r /tmp/requirements.txt
