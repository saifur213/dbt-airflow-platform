FROM apache/airflow:2.9.2-python3.11

USER root
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    libpq-dev \
    gcc \
    && apt-get clean

USER airflow

# Pin all versions for reproducibility
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

RUN pip install snowflake-sqlalchemy sqlalchemy

# Install dbt inside the same image for operator compatibility
RUN pip install --no-cache-dir \
    dbt-core==1.8.0 \
    dbt-postgres==1.8.0 \
    dbt-snowflake==1.8.0 \
    astronomer-cosmos==1.5.0