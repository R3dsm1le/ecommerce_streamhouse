"""
COMPONENTE: Visualización en Tiempo Real (dashboard/dashboard_app.py)
OBJETIVO: Dashboard mínimo en Streamlit que se refresque automáticamente.
"""
import streamlit as st
import duckdb
import time

st.title("⚡ Streamhouse E-Commerce en Vivo")

try:
    # Lee todos los archivos generados en tiempo real en data/ventas/
    df = duckdb.query("SELECT * FROM read_json_auto('data/ventas/*')").to_df()
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Ventas ($)", f"${df['total_ventas'].sum():,.2f}")
    with col2:
        st.metric("Órdenes Procesadas", f"{df['total_ordenes'].sum():,}")
        
    st.subheader("📦 Ventas por Producto")
    st.bar_chart(df.groupby("product")["total_ventas"].sum())
except Exception:
    st.warning("Esperando que Flink procese los primeros eventos y genere los archivos en data/ventas/...")

time.sleep(3)
st.rerun()
