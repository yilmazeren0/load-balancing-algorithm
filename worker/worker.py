from flask import Flask, request, jsonify
import time
import random
import os
import socket

app = Flask(__name__)

# Config
WORKER_ID = os.environ.get('WORKER_ID', 'unknown')
HOSTNAME = socket.gethostname()

@app.route('/process', methods=['POST'])
def process():
    data = request.json
    start_time = time.time()
    
    # Simulate Processing
    # Base latency + random noise
    sleep_time = random.uniform(0.01, 0.05)
    
    # Occasional "Hiccup" (simulating GC pause or complex task)
    if random.random() < 0.05:
        sleep_time += 0.2
        
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
