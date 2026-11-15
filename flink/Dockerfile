# ==============================================================================
# Dockerfile: Apache Flink con soporte para PyFlink, Kafka y Parquet
# ==============================================================================
FROM flink:1.18.1-scala_2.12-java11

# Instalar Python 3 y pip
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev \
        wget && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Instalar PyFlink
RUN pip3 install --no-cache-dir apache-flink==1.18.1

# Descargar conectores oficiales:
# 1. Conector Flink <-> Kafka
# 2. Soporte para escribir archivos en formato Parquet en el Lakehouse
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar && \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-parquet/1.18.1/flink-sql-parquet-1.18.1.jar

WORKDIR /opt/flink
