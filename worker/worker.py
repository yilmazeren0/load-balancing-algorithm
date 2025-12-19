from flask import Flask, request, jsonify
import time
import random
import os
import socket

app = Flask(__name__)

# Config
WORKER_ID = os.environ.get('WORKER_ID', 'unknown')
# Runtime Config
config = {
    'latency_min': 0.01,
    'latency_max': 0.05,
    'error_rate': 0.0,
    'cpu_factor': 1.0
}

@app.route('/config', methods=['POST'])
def update_config():
    global config
    new_conf = request.json
    config.update(new_conf)
    return jsonify({'status': 'updated', 'config': config})

@app.route('/process', methods=['POST'])
def process():
    data = request.json
    start_time = time.time()
    
    # Simulate Processing
    # Base latency + random noise
    sleep_time = random.uniform(config['latency_min'], config['latency_max'])
    
    # Occasional "Hiccup" (simulating GC pause or complex task)
    if random.random() < 0.05:
        sleep_time += 0.2
        
    # Simulate Error
    if random.random() < config['error_rate']:
        return jsonify({'error': 'simulated failure'}), 500
        
    time.sleep(sleep_time)
    
    duration = time.time() - start_time
    
    # Simple CPU emulation (just a number for now)
    cpu_usage = min(100.0, (duration * 1000) + random.uniform(-5, 5))
    
    return jsonify({
        'status': 'processed',
        'worker_id': WORKER_ID,
        'hostname': HOSTNAME,
        'duration_s': duration,
        'cpu_sim': cpu_usage
    })

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'healthy', 'id': WORKER_ID})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
