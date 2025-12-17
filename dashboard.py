import streamlit as st
import pandas as pd
import sqlite3
import time
import altair as alt

# Configuration
DB_FILE = 'metrics.db'
REFRESH_RATE_SEC = 2

st.set_page_config(page_title="Load Balancer Simulation", layout="wide")

def get_data(limit=500):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            query = f"SELECT * FROM metrics ORDER BY id DESC LIMIT {limit}"
            df = pd.read_sql_query(query, conn)
            # Normalize timestamp for better plotting if needed, 
            # currently just using raw timestamp or id for x-axis mostly
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            return df
    except Exception as e:
        st.error(f"Error reading database: {e}")
        return pd.DataFrame()

# Dashboard Layout
st.title("☁️ Cloud Load Balancing Simulation")

# Metrics Container
placeholder = st.empty()

while True:
    df = get_data()
    
    with placeholder.container():
        if not df.empty:
            current_algo = df.iloc[0]['algorithm']
            st.subheader(f"Current Algorithm: {current_algo}")
            
            # KPI Metrics
            avg_latency = df['processing_time'].mean() * 1000 # to ms
            avg_cpu = df['cpu_utilization'].mean()
            total_processed = len(df)
            
            kpi1, kpi2, kpi3 = st.columns(3)
            kpi1.metric(label="ISV Latency (avg)", value=f"{avg_latency:.2f} ms")
            kpi2.metric(label="Avg CPU Utilization", value=f"{avg_cpu:.1f} %")
            kpi3.metric(label="Data Points (Window)", value=total_processed)
            
            # Charts
            st.divider()
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### Real-time Latency per Worker")
                chart_latency = alt.Chart(df).mark_line().encode(
                    x='timestamp:T',
                    y='processing_time:Q',
                    color='worker_id:N',
                    tooltip=['worker_id', 'processing_time', 'timestamp']
                ).interactive()
                st.altair_chart(chart_latency, use_container_width=True)

            with col2:
                st.markdown("### Queue Depth per Worker")
                chart_queue = alt.Chart(df).mark_area(opacity=0.5).encode(
                    x='timestamp:T',
                    y='queue_depth:Q',
                    color='worker_id:N',
                    tooltip=['worker_id', 'queue_depth']
                ).interactive()
                st.altair_chart(chart_queue, use_container_width=True)
                
            st.markdown("### CPU Utilization per Worker")
            chart_cpu = alt.Chart(df).mark_bar().encode(
                x='worker_id:N',
                y='mean(cpu_utilization):Q',
                color='worker_id:N'
            )
            st.altair_chart(chart_cpu, use_container_width=True)

        else:
            st.info("Waiting for data... Ensure Producer and Load Balancer are running.")
            
    time.sleep(REFRESH_RATE_SEC)
