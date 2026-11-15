"""
================================================================================
PROYECTO: E-COMMERCE STREAMHOUSE
COMPONENTE: Procesador en Streaming (PyFlink)
ARCHIVO: flink/flink_processor.py
ROL: Mentoría Senior Data Engineer -> Junior Data Engineer
================================================================================
"""
from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Crear entorno de Table API en modo streaming
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# Activar Checkpointing cada 10 segundos para sellar archivos continuamente en disco
table_env.get_config().set("execution.checkpointing.interval", "10s")

# 2. Definir origen en Kafka con Watermarks de 5 segundos
table_env.execute_sql("""
    CREATE TABLE kafka_source (
        user_id INT,
        product STRING,
        price DOUBLE,
        `timestamp` STRING,
        event_time AS TO_TIMESTAMP(REPLACE(`timestamp`, 'T', ' ')),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'compras_topic',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'streamhouse_flink_group',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
""")

# 3. Definir destino Lakehouse (formato JSON lines en /data/ventas)
# Usar formato 'json' evita dependencias pesadas de Hadoop y es leído de forma nativa por DuckDB
table_env.execute_sql("""
    CREATE TABLE lakehouse_sink (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        product STRING,
        total_ventas DOUBLE,
        total_ordenes BIGINT
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file:///data/ventas',
        'format' = 'json'
    )
""")

# 4. Inserción continua con Ventana Tumbling de 1 minuto
table_result = table_env.execute_sql("""
    INSERT INTO lakehouse_sink
    SELECT 
        window_start,
        window_end,
        product, 
        ROUND(SUM(price), 2) AS total_ventas, 
        COUNT(*) AS total_ordenes
    FROM TABLE(
        TUMBLE(TABLE kafka_source, DESCRIPTOR(event_time), INTERVAL '1' MINUTES)
    )
    GROUP BY window_start, window_end, product
""")

print("[FLINK] Job de streaming lanzado exitosamente.")
# Mantener el proceso escuchando y procesando el flujo continuo
table_result.wait()
