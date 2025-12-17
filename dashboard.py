import streamlit as st
import pandas as pd
import sqlite3
import time
import altair as alt

# Configuration
DB_FILE = 'metrics.db'
REFRESH_RATE_SEC = 2

st.set_page_config(page_title="Load Balancer Battle (4-Way)", layout="wide")

def get_data(limit=2000):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                query = f"SELECT * FROM metrics ORDER BY id DESC LIMIT {limit}"
                df = pd.read_sql_query(query, conn)
                return df
            except Exception:
                return pd.DataFrame()
    except Exception as e:
        st.error(f"Error reading database: {e}")
        return pd.DataFrame()

def render_cluster_metrics(container, cluster_name, df_cluster):
    with container:
        # Determine Algorithm Name
        algo_name = "Unknown"
        if not df_cluster.empty:
            algo_name = df_cluster.iloc[0]['algorithm']
            
        st.subheader(f"{cluster_name}")
        st.caption(f"Algo: **{algo_name}**")
        
        if not df_cluster.empty:
            latency = df_cluster['processing_time'].mean() * 1000
            st.metric("Avg Latency", f"{latency:.2f} ms")
            
            # Latency Chart
            chart = alt.Chart(df_cluster).mark_line().encode(
                x=alt.X('timestamp:T', axis=alt.Axis(labels=False, title='')),
                y='processing_time:Q',
                color='worker_id:N',
                tooltip=['worker_id', 'processing_time']
            ).properties(height=200)
            st.altair_chart(chart, use_container_width=True)
            
            # Request Count Chart
            bar = alt.Chart(df_cluster).mark_bar().encode(
                x=alt.X('worker_id:N', axis=alt.Axis(labels=True, title='')),
                y='count():Q',
                color='worker_id:N'
            ).properties(height=150)
            st.altair_chart(bar, use_container_width=True)
        else:
            st.write("No Data")

# Dashboard Layout
st.title("⚔️ Load Balancing Algorithm Battle Royale")
st.markdown("Comparing **4 Algorithms** across **12 Workers** in Real-Time")

placeholder = st.empty()

while True:
    df = get_data()
    
    with placeholder.container():
        if not df.empty and 'cluster_id' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            
            # Split Data
            c_a = df[df['cluster_id'] == 'Cluster-A']
            c_b = df[df['cluster_id'] == 'Cluster-B']
            c_c = df[df['cluster_id'] == 'Cluster-C']
            c_d = df[df['cluster_id'] == 'Cluster-D']
            
            # 4 Columns
            col1, col2, col3, col4 = st.columns(4)
            
            render_cluster_metrics(col1, "Cluster A", c_a)
            render_cluster_metrics(col2, "Cluster B", c_b)
            render_cluster_metrics(col3, "Cluster C", c_c)
            render_cluster_metrics(col4, "Cluster D", c_d)

        else:
            if df.empty:
                st.info("Waiting for data... Ensure Producer and Load Balancer are running.")
            else:
                st.warning("Database schema updating... One moment.")
            
    time.sleep(REFRESH_RATE_SEC)
