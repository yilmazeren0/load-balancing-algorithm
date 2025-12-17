import streamlit as st
import pandas as pd
import sqlite3
import time
import altair as alt

# Configuration
DB_FILE = 'metrics.db'
REFRESH_RATE_SEC = 2

# Static Weights (same as load_balancer.py)
WEIGHTS = {
    'http://localhost:5004': 1, 
    'http://localhost:5005': 2, 
    'http://localhost:5006': 3
}

# Worker Definitions (To show 0 val for inactive ones)
WORKERS = {
    'Cluster-A': ['worker-1', 'worker-2', 'worker-3'],
    'Cluster-B': ['worker-4', 'worker-5', 'worker-6'],
    'Cluster-C': ['worker-7', 'worker-8', 'worker-9'],
    'Cluster-D': ['worker-10', 'worker-11', 'worker-12']
}

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
        st.subheader(f"{cluster_name}")
        
        # Determine Algorithm Name
        algo_name = df_cluster.iloc[0]['algorithm'] if not df_cluster.empty else "Waiting..."
        st.caption(f"Algo: **{algo_name}**")
        
        # Prepare Base Dataframe with ALL workers for this cluster
        all_workers = WORKERS.get(cluster_name, [])
        base_df = pd.DataFrame({'worker_id': all_workers})
        
        if not df_cluster.empty:
            latency = df_cluster['processing_time'].mean() * 1000
            st.metric("Avg Latency", f"{latency:.2f} ms")
            
            # Per-Worker Stats Calculation
            stats = df_cluster.groupby('worker_id').agg({
                'processing_time': 'mean',
                'id': 'count'
            }).reset_index()
            stats.columns = ['worker_id', 'avg_time_s', 'requests']
            
            # Merge with Base to show 0s
            merged = pd.merge(base_df, stats, on='worker_id', how='left').fillna(0)
            
            merged['Avg Response (ms)'] = merged['avg_time_s'] * 1000
            merged['Requests'] = merged['requests'].astype(int)
            merged['Weight'] = merged['worker_id'].apply(lambda w: WEIGHTS.get(w, 1)) # Mapping needs fix if logic uses localhost
            
            # Fix Weight mapping (the key in weights dict was full URL, let's just cheat for display)
            # Actually, let's map based on worker suffix for B
            def get_weight(w_id):
                if w_id == 'worker-4': return 1
                if w_id == 'worker-5': return 2
                if w_id == 'worker-6': return 3
                return 1
            merged['Weight'] = merged['worker_id'].apply(get_weight)
            
            # Display Table
            st.dataframe(
                merged[['worker_id', 'Avg Response (ms)', 'Weight', 'Requests']].style.format({
                    'Avg Response (ms)': '{:.2f}',
                    'Weight': '{:.0f}',
                    'Requests': '{:.0f}'
                }),
                hide_index=True,
                use_container_width=True
            )
            
            # Charts
            chart = alt.Chart(df_cluster).mark_line().encode(
                x=alt.X('timestamp:T', axis=alt.Axis(labels=False, title='')),
                y='processing_time:Q',
                color='worker_id:N'
            ).properties(height=150)
            st.altair_chart(chart, use_container_width=True)
            
        else:
            st.write("No Data")
            # Show empty table
            empty_df = pd.DataFrame({
                'worker_id': all_workers,
                'Avg Response (ms)': [0]*3,
                'Weight': [1]*3,
                'Requests': [0]*3
            })
            st.dataframe(empty_df, hide_index=True, use_container_width=True)

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
            
            render_cluster_metrics(col1, "Cluster-A", c_a)
            render_cluster_metrics(col2, "Cluster-B", c_b)
            render_cluster_metrics(col3, "Cluster-C", c_c)
            render_cluster_metrics(col4, "Cluster-D", c_d)

        else:
            if df.empty:
                st.info("Waiting for data... Ensure Producer and Load Balancer are running.")
            else:
                st.warning("Database schema updating... One moment.")
            
    time.sleep(REFRESH_RATE_SEC)
