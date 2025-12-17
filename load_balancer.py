import time
import json
import threading
import sqlite3
import random
import traceback
import requests
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Consumer, KafkaError

# Configuration
KAFKA_BROKER = 'localhost:9093'
INPUT_TOPIC = 'iot-sensor-data'
DB_FILE = 'metrics.db'

# Clusters
CLUSTER_A = 'Cluster-A' # Round Robin
CLUSTER_B = 'Cluster-B' # Weighted Round Robin
CLUSTER_C = 'Cluster-C' # Least Connections
CLUSTER_D = 'Cluster-D' # Least Response Time

# Algorithms
ALGO_ROUND_ROBIN = 'Round Robin'
ALGO_WEIGHTED_RR = 'Weighted Round Robin'
ALGO_LEAST_CONN = 'Least Connections'
ALGO_LEAST_RESPONSE = 'Least Response Time'

# Worker Config
workers_config = {
    CLUSTER_A: ['http://localhost:5001', 'http://localhost:5002', 'http://localhost:5003'],
    CLUSTER_B: ['http://localhost:5004', 'http://localhost:5005', 'http://localhost:5006'],
    CLUSTER_C: ['http://localhost:5007', 'http://localhost:5008', 'http://localhost:5009'],
    CLUSTER_D: ['http://localhost:5010', 'http://localhost:5011', 'http://localhost:5012']
}

# Static Weights for Cluster B
weights = {
    'http://localhost:5004': 1, 
    'http://localhost:5005': 2, 
    'http://localhost:5006': 3
}

# State Tracking
worker_response_times = {w: 0.05 for cluster in workers_config.values() for w in cluster}
active_connections = {w: 0 for cluster in workers_config.values() for w in cluster}

db_lock = threading.Lock()
stats_lock = threading.Lock()

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL,
                worker_id TEXT,
                cluster_id TEXT,
                processing_time REAL,
                algorithm TEXT
            )
        ''')
        conn.commit()

def log_metric(worker_id, cluster_id, processing_time, algo):
    try:
        with db_lock:
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO metrics (timestamp, worker_id, cluster_id, processing_time, algorithm)
                    VALUES (?, ?, ?, ?, ?)
                ''', (time.time(), worker_id, cluster_id, processing_time, algo))
                conn.commit()
    except Exception as e:
        print(f"Error logging: {e}")

def update_stats(worker_url, delta_conn=0, duration=None):
    with stats_lock:
        if delta_conn != 0:
            active_connections[worker_url] += delta_conn
        if duration is not None:
             # Exponential Moving Average for response time
             old_val = worker_response_times.get(worker_url, 0.05)
             worker_response_times[worker_url] = 0.7 * old_val + 0.3 * duration

def handle_request(worker_url, data, cluster_id, algorithm):
    try:
        # Increment active connections
        update_stats(worker_url, delta_conn=1)
        
        resp = requests.post(f"{worker_url}/process", json=data, timeout=5)
        
        # Decrement active connections
        update_stats(worker_url, delta_conn=-1)
        
        if resp.status_code == 200:
            res_json = resp.json()
            duration = res_json['duration_s']
            
            # Update response time stat
            update_stats(worker_url, duration=duration)
            
            # Log to DB
            log_metric(res_json['worker_id'], cluster_id, duration, algorithm)
            
    except Exception:
        # Decrement active connections on failure too
        update_stats(worker_url, delta_conn=-1)
        # Penalize failed worker slightly in stats? 
        # For now, just ignore.

def algo_round_robin(cluster_workers, counter):
    w = cluster_workers[counter % len(cluster_workers)]
    return w, (counter + 1)

def algo_weighted_rr(cluster_workers, counter):
    expanded = []
    for w in cluster_workers:
        w_val = weights.get(w, 1)
        for _ in range(w_val):
            expanded.append(w)
            
    if not expanded:
        return cluster_workers[0], counter
        
    w = expanded[counter % len(expanded)]
    return w, (counter + 1)

def algo_least_response(cluster_workers):
    with stats_lock:
        # Hibrit: Response Time * (1 + Active Connections)
        # Hizli ama dolu sunuculari cezalandirir.
        return min(cluster_workers, key=lambda w: worker_response_times.get(w, 1.0) * (1 + active_connections.get(w, 0)))

def algo_least_connections(cluster_workers):
    with stats_lock:
        # Add a tiny bit of random noise to break ties, otherwise it always picks the first one when all are 0
        return min(cluster_workers, key=lambda w: active_connections.get(w, 0))

def process_pipeline(cluster_id, algorithm, kafka_group_id):
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': kafka_group_id,
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe([INPUT_TOPIC])
    
    local_workers = workers_config[cluster_id]
    rr_counter = 0
    
    # Thread Pool for Async Requests
    # 5 threads per cluster is enough to show concurrency effects without overwhelming
    executor = ThreadPoolExecutor(max_workers=5) 
    
    print(f"[{cluster_id}] Started with {algorithm} (Workers: {len(local_workers)})")
    
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error(): continue
        
        try:
            data = json.loads(msg.value().decode('utf-8'))
            
            # Select Worker
            target_worker = local_workers[0]
            
            if algorithm == ALGO_ROUND_ROBIN:
                target_worker, rr_counter = algo_round_robin(local_workers, rr_counter)
            elif algorithm == ALGO_WEIGHTED_RR:
                target_worker, rr_counter = algo_weighted_rr(local_workers, rr_counter)
            elif algorithm == ALGO_LEAST_RESPONSE:
                target_worker = algo_least_response(local_workers)
            elif algorithm == ALGO_LEAST_CONN:
                target_worker = algo_least_connections(local_workers)
            
            # Submit to Executor (Async)
            executor.submit(handle_request, target_worker, data, cluster_id, algorithm)
            
        except Exception:
            traceback.print_exc()

def main():
    init_db()
    
    threads = []
    
    simulations = [
        (CLUSTER_A, ALGO_ROUND_ROBIN, 'group-a'),
        (CLUSTER_B, ALGO_WEIGHTED_RR, 'group-b'),
        (CLUSTER_C, ALGO_LEAST_CONN, 'group-c'),
        (CLUSTER_D, ALGO_LEAST_RESPONSE, 'group-d')
    ]
    
    for cluster, algo, group in simulations:
        t = threading.Thread(target=process_pipeline, args=(cluster, algo, group))
        t.start()
        threads.append(t)
        
    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
