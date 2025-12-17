import time
import json
import threading
import sqlite3
import random
import traceback
from queue import Queue
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
import collections

# Configuration
KAFKA_BROKER = 'localhost:9093'
INPUT_TOPIC = 'iot-sensor-data'
DB_FILE = 'metrics.db'

# Algorithms
ALGO_ROUND_ROBIN = 'Round Robin'
ALGO_LEAST_RESPONSE = 'Least Response Time'

# Global State
num_workers = 3
workers = [f"Worker-{i+1}" for i in range(num_workers)]
worker_queues = {w: Queue() for w in workers}
worker_stats = {w: {'cpu': 0.0, 'processing_time': 0.0, 'tasks_processed': 0} for w in workers}
current_algo = ALGO_ROUND_ROBIN 
db_lock = threading.Lock()

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL,
                worker_id TEXT,
                processing_time REAL,
                queue_depth INTEGER,
                cpu_utilization REAL,
                algorithm TEXT
            )
        ''')
        conn.commit()

def log_metric(worker_id, processing_time, queue_depth, cpu_utilization):
    try:
        with db_lock:
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO metrics (timestamp, worker_id, processing_time, queue_depth, cpu_utilization, algorithm)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (time.time(), worker_id, processing_time, queue_depth, cpu_utilization, current_algo))
                conn.commit()
    except Exception as e:
        print(f"Error logging metric: {e}")

def worker_thread(worker_id):
    """Simulates a worker processing tasks."""
    while True:
        try:
            task = worker_queues[worker_id].get(timeout=1)
            
            # Simulate CPU load and processing time
            start_time = time.time()
            
            # Processing time affected by "CPU load"
            base_processing_time = random.uniform(0.01, 0.05) 
            if random.random() < 0.1:
                base_processing_time *= 5
            
            time.sleep(base_processing_time)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Update internal stats
            worker_stats[worker_id]['processing_time'] = duration
            worker_stats[worker_id]['cpu'] = min(100.0, (duration * 1000) + random.uniform(-10, 10))
            worker_stats[worker_id]['tasks_processed'] += 1

            # Log to DB
            q_depth = worker_queues[worker_id].qsize()
            log_metric(worker_id, duration, q_depth, worker_stats[worker_id]['cpu'])
            
            worker_queues[worker_id].task_done()
            
        except Exception:
            pass

def round_robin_balancer(rr_counter):
    worker_id = workers[rr_counter % num_workers]
    return worker_id, (rr_counter + 1)

def least_response_time_balancer():
    best_worker = min(workers, key=lambda w: worker_stats[w]['processing_time'])
    return best_worker

def consume_and_balance():
    global current_algo
    
    init_db()
    rr_counter = 0

    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'load-balancer-group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }

    consumer = Consumer(conf)
    
    try:
        consumer.subscribe([INPUT_TOPIC])
        
        # Start Worker Threads
        for w in workers:
            t = threading.Thread(target=worker_thread, args=(w,), daemon=True)
            t.start()
        
        print(f"Load Balancer started. Listening on {INPUT_TOPIC}...")
        
        start_time = time.time()

        while True:
            # Poll for message
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    continue

            # Switch algorithm every 30 seconds
            elapsed = time.time() - start_time
            if int(elapsed / 30) % 2 == 0:
                current_algo = ALGO_ROUND_ROBIN
            else:
                current_algo = ALGO_LEAST_RESPONSE

            # Process Message
            try:
                data = json.loads(msg.value().decode('utf-8'))
                
                # Select Worker
                if current_algo == ALGO_ROUND_ROBIN:
                    target_worker, rr_counter = round_robin_balancer(rr_counter)
                else:
                    target_worker = least_response_time_balancer()
                
                worker_queues[target_worker].put(data)
                
            except json.JSONDecodeError:
                print(f"Invalid JSON: {msg.value()}")

    except KeyboardInterrupt:
        print("Stopping Load Balancer...")
    except Exception as e:
        print(f"Error in Load Balancer: {e}")
        traceback.print_exc()
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_and_balance()
