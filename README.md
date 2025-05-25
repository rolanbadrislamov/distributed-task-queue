# Distributed Task Queue System

This project implements a distributed task queue system using FastAPI and Redis, demonstrating practical approaches to scalability in distributed systems. It's designed as part of a bachelor's thesis to showcase distributed system concepts without relying on existing task queue solutions like Celery.

## Architecture

The system consists of the following components:

1. **Task Queue API**: FastAPI-based REST API for task submission and management
2. **Task Workers**: Distributed workers that process tasks from the queue
3. **Redis Backend**: For task storage, distribution, and state management
4. **Result Backend**: For storing task results and status
5. **Monitoring Stack**: Prometheus and Grafana for real-time metrics
6. **Partition Handler**: For managing network partitions and split-brain scenarios

## Features

### Core Features
- Distributed task processing
- Horizontal scalability
- Fault tolerance and task recovery
- Real-time task status monitoring
- Priority queue support
- Task result storage and retrieval
- Dead letter queue for failed tasks
- Task retries with backoff

### Advanced Scalability Features
- Circuit Breaker pattern for failure isolation
- Back Pressure mechanism for load management
- Partition tolerance with automatic recovery
- Real-time metrics collection and visualization
- Dynamic worker scaling
- Comprehensive system monitoring

## Project Structure

```
distributed-task-queue/
├── app/
│   ├── api/              # FastAPI routes and endpoints
│   ├── core/             # Core task queue implementation
│   │   ├── circuit_breaker.py    # Circuit breaker pattern
│   │   ├── back_pressure.py      # Back pressure mechanism
│   │   ├── partition_handler.py  # Network partition handling
│   │   └── metrics_collector.py  # System metrics collection
│   ├── models/           # Pydantic models
│   └── workers/          # Task worker implementation
├── monitoring/
│   ├── grafana/         # Grafana dashboards
│   └── prometheus/      # Prometheus configuration
├── experiments/         # Load testing and experiments
│   ├── load_testing/   # Load test scenarios
│   └── results/        # Test results and analysis
├── tests/              # Test cases
├── docker-compose.yml  # Docker composition
├── requirements.txt    # Python dependencies
├── Dockerfile.redis    # Redis configuration
└── README.md          # Project documentation
```

## Setup and Installation

### Prerequisites
- Python 3.9+
- Docker and Docker Compose
- Redis 7.2+
- Prometheus and Grafana (for monitoring)

### Redis Setup

1. Build the Redis Docker image:
   ```bash
   docker build -t distributed-task-queue-redis -f Dockerfile.redis .
   ```

2. Run the Redis container:
   ```bash
   docker run -d \
     --name task-queue-redis \
     -p 6379:6379 \
     -v redis-data:/redis/data \
     distributed-task-queue-redis
   ```

### System Setup

1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Start the entire stack:
   ```bash
   docker-compose up -d
   ```

## Usage

### Using the API

1. Submit a task:
   ```bash
   curl -X POST "http://localhost:8000/tasks/" -H "Content-Type: application/json" -d '{"task_type": "fibonacci", "payload": {"n": 10}, "priority": 1, "max_retries": 3}'
   ```

2. Check task status:
   ```bash
   curl "http://localhost:8000/tasks/{task_id}"
   ```

3. Get queue statistics:
   ```bash
   curl "http://localhost:8000/tasks/stats"
   ```

## Scalability Features

### Circuit Breaker
- Prevents cascading failures
- Automatic recovery mechanism
- Configurable failure thresholds
- Three states: CLOSED, OPEN, HALF-OPEN

### Back Pressure
- Prevents system overload
- Adaptive request throttling
- Resource-aware delay calculation
- Configurable thresholds

### Partition Tolerance
- Network partition detection
- Automatic recovery
- Split-brain prevention
- Node health monitoring

## Monitoring and Metrics

### Accessing Monitoring Tools

1. **Grafana Dashboard**
   - URL: `http://localhost:3000`
   - Default credentials:
     - Username: `admin`
     - Password: `admin`
   - Available dashboards:
     - Task Queue Overview
     - Worker Performance
     - System Resources
     - Redis Metrics

2. **Prometheus Metrics**
   - URL: `http://localhost:9090`
   - Direct metrics endpoint: `http://localhost:8000/metrics`
   - Key metrics available:
     ```
     # Task queue metrics
     task_total{status="completed"}
     task_total{status="failed"}
     queue_size
     
     # Worker metrics
     worker_load
     active_workers
     task_processing_seconds
     
     # System metrics
     memory_usage_bytes
     cpu_usage_percent
     ```

3. **Health Check Endpoints**
   ```bash
   # API health status
   curl http://localhost:8000/health
   
   # Redis connection status
   docker exec task-queue-redis redis-cli ping
   
   # Worker status
   curl http://localhost:8000/workers/status
   ```

### Monitoring Dashboard Guide

1. **Task Queue Overview Dashboard**
   - Real-time queue size
   - Task processing rate
   - Error rates
   - Average processing time
   
   To access:
   1. Open Grafana (`http://localhost:3000`)
   2. Navigate to Dashboards → Task Queue
   3. Select time range (default: last 1 hour)

2. **Worker Performance Dashboard**
   - Active worker count
   - Worker load distribution
   - Task processing latency
   - Error distribution
   
   To access:
   1. Open Grafana
   2. Navigate to Dashboards → Worker Performance
   3. Filter by worker ID or time range

3. **System Resources Dashboard**
   - CPU usage per worker
   - Memory consumption
   - Network I/O
   - Redis memory usage
   
   To access:
   1. Open Grafana
   2. Navigate to Dashboards → System Resources
   3. View overall or per-component metrics

### Setting Up Alerts

1. **Grafana Alerts**
   1. Navigate to Alerting in Grafana
   2. Click "New Alert Rule"
   3. Configure conditions, e.g.:
      ```yaml
      - Alert: HighQueueSize
        Condition: queue_size > 1000
        Duration: 5m
      
      - Alert: WorkerOverload
        Condition: worker_load > 0.8
        Duration: 2m
      ```

2. **Email Notifications**
   ```yaml
   # In Grafana UI:
   1. Configure → Notification channels
   2. Add Email channel
   3. Set email server settings
   4. Test notification
   ```

### Metrics Collection

1. **System Metrics**
   - Collected every 5 seconds
   - Stored in Prometheus
   - Retention period: 15 days
   - Access via API:
     ```bash
     # Get current metrics
     curl http://localhost:8000/metrics
     
     # Get historical metrics
     curl http://localhost:8000/metrics/history?hours=24
     ```

2. **Custom Metrics**
   - Task success rate
   - Queue growth rate
   - Worker efficiency
   - Access via API:
     ```bash
     # Get task processing statistics
     curl http://localhost:8000/metrics/tasks
     
     # Get worker statistics
     curl http://localhost:8000/metrics/workers
     ```

### Troubleshooting Metrics

1. **Missing Data**
   ```bash
   # Check Prometheus targets
   curl http://localhost:9090/targets
   
   # Check metrics endpoint
   curl http://localhost:8000/metrics
   ```

2. **Grafana Issues**
   ```bash
   # Verify Prometheus data source
   curl http://localhost:9090/-/healthy
   
   # Check Grafana logs
   docker-compose logs grafana
   ```

3. **Redis Metrics**
   ```bash
   # Check Redis info
   docker exec task-queue-redis redis-cli info
   
   # Monitor Redis in real-time
   docker exec task-queue-redis redis-cli monitor
   ```

### Exporting Metrics

1. **CSV Export**
   - Via Grafana:
     1. Open desired dashboard
     2. Click panel title → More → Export CSV
   - Via API:
     ```bash
     curl http://localhost:8000/metrics/export?format=csv > metrics.csv
     ```

2. **JSON Export**
   ```bash
   # Export all metrics
   curl http://localhost:8000/metrics/export?format=json > metrics.json
   
   # Export specific time range
   curl "http://localhost:8000/metrics/export?format=json&start=$(date -d '24 hours ago' +%s)&end=$(date +%s)" > last_24h_metrics.json
   ```

## Load Testing and Experiments

### Running Experiments
```bash
python -m experiments.run_experiments
```

### Available Test Scenarios
- Baseline performance
- High load conditions
- Stress testing
- Network partition scenarios
- Recovery testing

### Metrics Collected
- Request latency (avg, p95, p99)
- Throughput (requests/second)
- Error rates
- Resource utilization
- Queue growth rates

## Contributing

This project is part of a bachelor's thesis on distributed systems. Contributions and suggestions are welcome.

## License

MIT License 