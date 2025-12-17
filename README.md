# Big Data Architecture Simulation (Massive Scale)

This project simulates a real-time data pipeline for comparative load balancing analysis.

## Architecture
1.  **Producer (`producer.py`)**: Simulates IoT sensors sending data to Kafka (Port 9093).
2.  **Workers (Docker)**: **12 separate Flask-based containers** simulating cloud servers.
    -   **Cluster A**: Workers 1-3 (Round Robin)
    -   **Cluster B**: Workers 4-6 (Weighted Round Robin)
    -   **Cluster C**: Workers 7-9 (Least Connections)
    -   **Cluster D**: Workers 10-12 (Least Response Time)
3.  **Load Balancer (`load_balancer.py`)**: Consumes from Kafka, distributes requests via HTTP, tracks metrics.
4.  **Dashboard (`dashboard.py`)**: Visualizes real-time performance comparison.

## Prerequisites
- Docker & Docker Compose
- Python 3.9+ (8GB RAM recommended)

## Setup

1.  **Start Infrastructure**
    ```bash
    docker-compose up -d --build
    ```

2.  **Install Dependencies**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

## Usage

Run the following commands in separate terminal windows (with `venv` active):

1.  **Start Producer**
    ```bash
    python producer.py
    ```

2.  **Start Load Balancer**
    ```bash
    python load_balancer.py
    ```
    *This script runs 4 internal threads to drive all 4 clusters simultaneously.*

3.  **Start Dashboard**
    ```bash
    streamlit run dashboard.py
    ```

## Comparison
The dashboard displays 4 columns, one for each algorithm, allowing you to see which strategy handles load best under various conditions.
