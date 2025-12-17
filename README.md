# Big Data Architecture Simulation (Kafka + Streamlit)

This project simulates a real-time data pipeline for load balancing optimization.

## Architecture
1.  **Producer (`producer.py`)**: Simulates 50+ IoT sensors sending JSON data to Kafka.
2.  **Middleware (Kafka)**: Buffers incoming data.
3.  **Load Balancer (`load_balancer.py`)**: Consumes data, distributes to worker threads using Round Robin / Least Response Time, and logs metrics to SQLite.
4.  **Dashboard (`dashboard.py`)**: Visualizes real-time metrics.

## Prerequisites
- Docker & Docker Compose
- Python 3.9+
- Kafka (provided via Docker)

## Setup

1.  **Start Infrastructure**
    ```bash
    docker-compose up -d
    ```
    *Access Kafka UI at [http://localhost:8080](http://localhost:8080).*

2.  **Install Dependencies**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

## Usage

Run the following commands in separate terminal windows (make sure `venv` is activated in each):

1.  **Start Producer**
    ```bash
    python producer.py
    ```

2.  **Start Load Balancer**
    ```bash
    python load_balancer.py
    ```

3.  **Start Dashboard**
    ```bash
    streamlit run dashboard.py
    ```

## Troubleshooting
- If Kafka fails to connect, ensure the Docker containers are healthy: `docker-compose ps`.
- If Streamlit doesn't show data, wait a few seconds for the `producer` and `load_balancer` to populate the database.
