import argparse
import requests
import json
import time

# Configuration
WORKERS_CONFIG = {
    'Cluster-A': ['http://localhost:5001', 'http://localhost:5002', 'http://localhost:5003'],
    'Cluster-B': ['http://localhost:5004', 'http://localhost:5005', 'http://localhost:5006'],
    'Cluster-C': ['http://localhost:5007', 'http://localhost:5008', 'http://localhost:5009'],
    'Cluster-D': ['http://localhost:5010', 'http://localhost:5011', 'http://localhost:5012']
}

ALL_WORKERS = [w for cluster in WORKERS_CONFIG.values() for w in cluster]

def update_producer(num_sensors, interval_ms):
    config = {'num_sensors': num_sensors, 'interval_ms': interval_ms}
    with open('producer_config.json', 'w') as f:
        json.dump(config, f)
    print(f"Producer config updated: {config}")

def update_worker(worker_url, latency_min=0.01, latency_max=0.05, error_rate=0.0):
    try:
        data = {
            'latency_min': latency_min,
            'latency_max': latency_max,
            'error_rate': error_rate
        }
        resp = requests.post(f"{worker_url}/config", json=data, timeout=1)
        if resp.status_code == 200:
            print(f"Updated {worker_url}: {data}")
        else:
            print(f"Failed to update {worker_url}: {resp.status_code}")
    except Exception as e:
        print(f"Error updating {worker_url}: {e}")

def scenario_uniform():
    print("Applying Scenario: Uniform Load & Performance")
    update_producer(num_sensors=50, interval_ms=100)
    for w in ALL_WORKERS:
        update_worker(w, latency_min=0.01, latency_max=0.05, error_rate=0.0)

def scenario_high_load():
    print("Applying Scenario: High Load")
    update_producer(num_sensors=200, interval_ms=50)
    for w in ALL_WORKERS:
        update_worker(w, latency_min=0.01, latency_max=0.05)

def scenario_latency_spike(cluster='Cluster-C'):
    print(f"Applying Scenario: Latency Spike in {cluster}")
    # Pick one worker in the cluster to be slow
    target_cluster = WORKERS_CONFIG.get(cluster, [])
    if not target_cluster:
        print("Cluster not found")
        return
        
    slow_worker = target_cluster[0]
    normal_workers = [w for w in ALL_WORKERS if w != slow_worker]
    
    # Slow worker
    update_worker(slow_worker, latency_min=0.5, latency_max=0.8)
    
    # Others normal
    for w in normal_workers:
        update_worker(w, latency_min=0.01, latency_max=0.05)
        
    update_producer(num_sensors=50, interval_ms=100)

def scenario_chaos():
    print("Applying Scenario: Chaos (Random Failures)")
    for w in ALL_WORKERS:
        # 10% failure rate
        update_worker(w, error_rate=0.1)
    update_producer(num_sensors=50, interval_ms=100)

def main():
    parser = argparse.ArgumentParser(description='Load Balancer Scenario Manager')
    parser.add_argument('mode', choices=['uniform', 'high_load', 'latency_spike', 'chaos'], help='Scenario mode to apply')
    parser.add_argument('--cluster', default='Cluster-C', help='Target cluster for latency spike')
    
    args = parser.parse_args()
    
    if args.mode == 'uniform':
        scenario_uniform()
    elif args.mode == 'high_load':
        scenario_high_load()
    elif args.mode == 'latency_spike':
        scenario_latency_spike(cluster=args.cluster)
    elif args.mode == 'chaos':
        scenario_chaos()

if __name__ == '__main__':
    main()
