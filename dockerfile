FROM apache/airflow:2.8.4

USER root
RUN mkdir -p /opt/airflow/drivers && \
    wget https://jdbc.postgresql.org/download/postgresql-42.7.2.jar -O /opt/airflow/drivers/postgresql-jdbc.jar

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        curl \
        wget \
        procps \
        netcat-openbsd && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN echo '#!/bin/bash' > /usr/local/bin/set-java-home && \
    echo 'export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))' >> /usr/local/bin/set-java-home && \
    echo 'export PATH=$JAVA_HOME/bin:$PATH' >> /usr/local/bin/set-java-home && \
    chmod +x /usr/local/bin/set-java-home

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

RUN pip install --no-cache-dir --timeout=1000 \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    pyspark==3.5.0 \
    findspark \
    apache-airflow-providers-apache-spark