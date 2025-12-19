import time
import json
import random
import uuid
from confluent_kafka import Producer
import socket

# Configuration
KAFKA_BROKER = 'localhost:9093'
TOPIC = 'iot-sensor-data'

def get_producer():
    try:
        conf = {
            'bootstrap.servers': KAFKA_BROKER,
            'client.id': socket.gethostname()
        }
        producer = Producer(conf)
        return producer
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return None

def delivery_callback(err, msg):
    if err:
        print(f'Message failed delivery: {err}')
    # else:
    #     print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def simulated_sensor_data(sensor_id):
    """Generates simulated sensor data."""
    return {
        'sensor_id': sensor_id,
        'temperature': round(random.uniform(20.0, 30.0), 2),
        'timestamp': time.time()
    }

def load_config():
    try:
        with open('producer_config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {'interval_ms': 100, 'num_sensors': 50}
    except Exception:
        return None

def run_simulation(default_num_sensors=50, default_interval_ms=100):
    producer = get_producer()
    if not producer:
        print("Could not create Kafka producer. Exiting.")
        return

    # Initial config
    config = load_config()
    num_sensors = config.get('num_sensors', default_num_sensors)
    interval_ms = config.get('interval_ms', default_interval_ms)

    print(f"Starting simulation. Initial config: {num_sensors} sensors, {interval_ms}ms interval.")
    
    # Generate stable IDs for sensors
    # Generate stable IDs for sensors (max possible to avoid churn)
    # We generate a large pool and pick based on current config
    sensor_pool = [str(uuid.uuid4()) for _ in range(500)] 
    
    try:
        while True:
            # We use active_sensors from the updated block below for next iteration,
            # but for the very first loop we need to define it or reorganize.
            # Simplified:
            current_config = load_config() or {'num_sensors': default_num_sensors, 'interval_ms': default_interval_ms}
            
            num_s = current_config.get('num_sensors', default_num_sensors)
            interval = current_config.get('interval_ms', default_interval_ms)
            
            active_ids = sensor_pool[:num_s]
            
            for sid in active_ids:
                data = simulated_sensor_data(sid)
                value_json = json.dumps(data).encode('utf-8')
                producer.produce(TOPIC, value=value_json, callback=delivery_callback)
            
            # Serve delivery reports triggered by produce()
            producer.poll(0)
            
            # Dynamic Config Update
            new_config = load_config()
            if new_config:
                interval_ms = new_config.get('interval_ms', interval_ms)
                # Adjust active sensors
                current_num = new_config.get('num_sensors', num_sensors)
                active_sensors = sensor_pool[:current_num]
            else:
                active_sensors = sensor_pool[:num_sensors]

            time.sleep(interval_ms / 1000.0)
    except KeyboardInterrupt:
        print("\nStopping simulation.")
    except Exception as e:
        print(f"Error during simulation: {e}")
    finally:
        if producer:
            producer.flush()

if __name__ == "__main__":
    run_simulation()
