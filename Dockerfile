FROM apache/airflow:3.2.2-python3.11

RUN pip install --no-cache-dir \
    pandas==2.2.2 \
    openpyxl \
    psycopg2-binary \
    apache-airflow-providers-postgres \
    apache-airflow-providers-common-sql \
    apache-airflow-providers-fab