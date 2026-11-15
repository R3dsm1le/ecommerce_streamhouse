"""
COMPONENTE: Analítica Lakehouse (data_processing/data_processing.py)
OBJETIVO: Consultar las tablas generadas por Flink usando Databricks SDK / DuckDB.
"""
from databricks.sdk import WorkspaceClient

# 1. Conectar al endpoint local de MiniLake
w = WorkspaceClient(host="http://localhost:8000", token="dev")

# 2. Obtener el SQL Warehouse activo en MiniLake
warehouses = list(w.warehouses.list())
if not warehouses:
    print("[MINILAKE] Creando SQL Warehouse local...")
    wh = w.warehouses.create(name="local-warehouse", cluster_size="2X-Small").result()
    warehouse_id = wh.id
else:
    warehouse_id = warehouses[0].id

print(f"[MINILAKE] Conectado al Warehouse ID: {warehouse_id}")

# 3. Consulta Analítica de KPIs sobre los archivos en streaming
query = """
    SELECT 
        product, 
        ROUND(SUM(total_ventas), 2) AS ventas_totales,
        SUM(total_ordenes) AS ordenes_totales
    FROM read_json_auto('/data/ventas/*')
    GROUP BY product
    ORDER BY ventas_totales DESC;
"""

# 4. Ejecutar la consulta en MiniLake (con motor DuckDB)
try:
    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=query
    )
    
    print("\n" + "="*50)
    print("📊 REPORTE DE VENTAS EN TIEMPO REAL (LAKEHOUSE)")
    print("="*50)
    
    # Extraer las columnas y filas de la respuesta
    columns = [col.name for col in response.manifest.schema.columns]
    data = response.result.data_array
    
    header = f"{columns[0]:<15} | {columns[1]:<16} | {columns[2]:<15}"
    print(header)
    print("-" * len(header))
    for row in data:
        print(f"{row[0]:<15} | ${float(row[1]):<15,.2f} | {row[2]:<15}")
    print("="*50 + "\n")
    
except Exception as e:
    print(f"Error al consultar MiniLake: {e}")
