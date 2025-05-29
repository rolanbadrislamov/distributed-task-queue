# Distributed Task Queue System

This project implements a distributed task queue system using FastAPI and Redis. It serves as a practical demonstration of distributed system concepts, focusing on scalability, fault tolerance, and real-time monitoring, developed as part of a bachelor's thesis. The system is built from fundamental components to illustrate these concepts without relying on off-the-shelf solutions like Celery.

## Core Objectives
- To design and implement a scalable task processing system.
- To explore and implement mechanisms for fault tolerance (e.g., task retries).
- To integrate real-time monitoring for system performance and health.
- To demonstrate dynamic worker scaling and its impact on system throughput.

## System Architecture

The system comprises the following key components:

1.  **Task Queue API**: A FastAPI-based RESTful service for submitting tasks, checking task status, and retrieving results.
2.  **Task Workers**: Independent worker processes that fetch tasks from the queue, execute them, and report results. These are designed to be horizontally scalable.
3.  **Redis Backend**: Utilized as a message broker for the task queue, managing task distribution and persistence. It also serves for storing intermediate task states.
4.  **Monitoring Stack**: Prometheus for metrics collection and Grafana for visualization, providing insights into system behavior and performance.

## Key Features

### Implemented Features
-   **Distributed Task Processing**: Tasks are distributed among multiple worker instances.
-   **Horizontal Worker Scaling**: Workers can be scaled dynamically (e.g., using Docker Compose) to handle varying loads.
-   **Task Submission API**: Endpoints for creating and managing tasks.
-   **Basic Fault Tolerance**: Includes task retries on failure.
-   **Real-time Metrics Collection**: Prometheus scrapes metrics from the API and workers.
-   **Monitoring Dashboards**: Grafana dashboards to visualize key performance indicators.
-   **Experimentation Framework**: Scripts to run and analyze performance under different configurations (e.g., varying number of workers).

### Explored Concepts (may require further development for production readiness)
-   **Circuit Breaker Pattern**: `app/core/circuit_breaker.py` provides an implementation to prevent cascading failures.
-   **Back Pressure Mechanism**: `app/core/back_pressure.py` includes logic for managing system load.
-   **Partition Handling**: `app/core/partition_handler.py` explores concepts for dealing with network partitions.

```
distributed-task-queue/
├── app/
│   ├── main.py           # FastAPI application entry point, API routes
│   ├── api/              # API specific logic (if separated from main.py)
│   ├── core/             # Core logic: queue, metrics, patterns
│   │   ├── back_pressure.py
│   │   ├── circuit_breaker.py
│   │   ├── metrics_collector.py
│   │   ├── partition_handler.py
│   │   └── queue.py
│   ├── models/           # Pydantic models for tasks and API requests/responses
│   │   └── task.py
│   └── workers/          # Task worker implementation
│       ├── task_handlers.py # Logic for specific task types
│       └── worker.py        # Main worker process
├── experiments/          # Scripts for performance experiments
│   ├── baseline_experiment.py
│   ├── high_load_experiment.py
│   ├── run_experiments.py
│   ├── stress_test_experiment.py
│   └── worker_scaling_experiment.py
├── experiment_results/   # Directory for storing results from experiments
├── monitoring/
│   ├── grafana/          # Grafana configuration and dashboards
│   │   ├── dashboards/
│   │   │   ├── default.yml
│   │   │   └── task_queue_metrics.json
│   │   └── provisioning/
│   └── prometheus.yml      # Prometheus configuration file
├── docker-compose.yml  # Docker composition
├── Dockerfile            # Dockerfile for the main application (API and workers)
├── Dockerfile.redis      # Dockerfile for custom Redis image (if used)
├── redis.conf            # Custom Redis configuration
├── requirements.txt    # Python dependencies
└── README.md          # Project documentation
```

## Setup and Installation

### Prerequisites
- Python 3.9+
- Docker and Docker Compose

### Running the System

1.  **Clone the repository (if you haven't already):**
    ```bash
    git clone <repository-url>
    cd distributed-task-queue
    ```

2.  **Install Python dependencies (optional, if running outside Docker for development):**
   ```bash
   pip install -r requirements.txt
   ```

3.  **Build and start the services using Docker Compose:**
    This command will build the necessary Docker images (if not already built) and start the API, workers, Redis, Prometheus, and Grafana containers.
   ```bash
   docker-compose up -d
    ```
    To scale the number of workers (e.g., to 3 workers):
    ```bash
    docker-compose up -d --build --scale worker=3
   ```

## Usage

### API Endpoints

The API server runs on `http://localhost:8000` by default (as per `docker-compose.yml`).

-   **Submit a Task:**
   ```bash
   curl -X POST "http://localhost:8000/tasks/" \
         -H "Content-Type: application/json" \
         -d '{"task_type": "matrix_multiply", "payload": {"size": 10}, "priority": 1, "max_retries": 3}'
   ```
    *(Example payload; actual task types and payloads depend on `app/workers/task_handlers.py`)*

-   **Check Task Status (Example - actual endpoint might vary):**
   ```bash
   curl "http://localhost:8000/tasks/{task_id}"
   ```

-   **View API Documentation (Swagger UI):**
    Open `http://localhost:8000/docs` in your browser.

### Monitoring

-   **Prometheus:**
    -   Access the Prometheus UI at `http://localhost:9090`.
    -   Metrics from the API and workers are exposed at their respective `/metrics` endpoints (e.g., `http://localhost:8000/metrics`).
-   **Grafana:**
    -   Access Grafana at `http://localhost:3000`.
    -   Default credentials are typically `admin` / `admin`.
    -   A pre-configured dashboard for task queue metrics should be available under `monitoring/grafana/dashboards/`.

## Load Testing and Experiments

The `experiments/` directory contains Python scripts to simulate load and measure system performance under various conditions.

-   **Running Experiments:**
    Navigate to the `experiments` directory and run the desired script. For example, to run the worker scaling experiment:
    ```bash
    python experiments/worker_scaling_experiment.py
    ```
    The main script to run all experiments might be:
    ```bash
    python experiments/run_experiments.py
    ```
    Refer to the individual experiment scripts for specific configurations. Results are typically saved in the `experiment_results/` directory.

-   **Available Experiments:**
    -   `worker_scaling_experiment.py`: Tests system performance with varying numbers of worker instances.
    -   `baseline_experiment.py`: Establishes baseline performance metrics.
    -   `high_load_experiment.py`: Simulates high load scenarios.
    -   `stress_test_experiment.py`: Pushes the system to its limits to identify bottlenecks.

## For the Thesis

This project aims to:
-   Demonstrate an understanding of distributed system principles.
-   Provide a platform for experimenting with scalability and fault-tolerance techniques.
-   Showcase the ability to integrate monitoring tools for system analysis.
-   The results from the `experiments/` can be used to analyze and discuss the system's behavior, scalability, and limitations.

## Future Work & Potential Enhancements
-   More sophisticated error handling and dead-letter queue implementation.
-   Advanced task prioritization strategies.
-   Implementation of leader election for workers or specific roles.
-   More comprehensive security measures.
-   Dynamic resource allocation based on queue length or processing times.

## Contributing

This project is part of a bachelor's thesis on distributed systems. Contributions and suggestions are welcome.

## License

MIT License 