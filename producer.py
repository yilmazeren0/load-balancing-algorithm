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

def run_simulation(num_sensors=50, interval_ms=100):
    producer = get_producer()
    if not producer:
        print("Could not create Kafka producer. Exiting.")
        return

    print(f"Starting simulation with {num_sensors} sensors sending data every {interval_ms}ms...")
    
    # Generate stable IDs for sensors
    sensor_ids = [str(uuid.uuid4()) for _ in range(num_sensors)]
    
    try:
        while True:
            for sid in sensor_ids:
                data = simulated_sensor_data(sid)
                value_json = json.dumps(data).encode('utf-8')
                producer.produce(TOPIC, value=value_json, callback=delivery_callback)
            
            # Serve delivery reports triggered by produce()
            producer.poll(0)
            
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
