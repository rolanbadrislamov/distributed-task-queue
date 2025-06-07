from fastapi import APIRouter
from fastapi.responses import Response, JSONResponse
from typing import Dict
import psutil
import os
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter, Gauge, REGISTRY, CollectorRegistry

from app.core.queue import TaskQueue

# Initialize Prometheus metrics with a new registry
REGISTRY = CollectorRegistry()
REQUESTS = Counter('http_requests_total', 'Total HTTP requests', registry=REGISTRY)
QUEUE_SIZE = Gauge('queue_size', 'Current size of task queue', registry=REGISTRY)
DEAD_LETTER_QUEUE_SIZE = Gauge('dead_letter_queue_size', 'Size of dead letter queue', registry=REGISTRY)

# New metrics for worker stats
ACTIVE_WORKERS = Gauge('active_workers', 'Number of active workers', registry=REGISTRY)
WORKER_MEMORY_USAGE = Gauge('worker_memory_usage_bytes', 'Worker memory usage in bytes', registry=REGISTRY)
WORKER_CPU_LOAD = Gauge('worker_cpu_load_percent', 'Worker CPU load percentage', registry=REGISTRY)
TASKS_IN_PROGRESS = Gauge('tasks_in_progress', 'Number of tasks currently being processed', registry=REGISTRY)

router = APIRouter(tags=["monitoring"])

# This will be injected as dependency
task_queue: TaskQueue = None

def set_task_queue(queue: TaskQueue):
    """Set the task queue instance for this router"""
    global task_queue
    task_queue = queue

@router.get("/metrics")
async def prometheus_metrics():
    """
    Prometheus metrics endpoint - returns metrics in Prometheus format
    """
    # Update metrics with current values
    try:
        # Get queue statistics
        queue_length = await task_queue.get_queue_length()
        dead_letter_queue_length = await task_queue.get_dead_letter_queue_length()
        
        # Get process metrics
        process = psutil.Process(os.getpid())
        memory_usage = process.memory_info().rss
        cpu_percent = process.cpu_percent()
        
        # Update Prometheus metrics
        QUEUE_SIZE.set(queue_length)
        DEAD_LETTER_QUEUE_SIZE.set(dead_letter_queue_length)
        WORKER_MEMORY_USAGE.set(memory_usage)
        WORKER_CPU_LOAD.set(cpu_percent)
        
        # Get active workers count from Redis
        active_workers = await task_queue.get_active_workers_count()
        ACTIVE_WORKERS.set(active_workers)
        
        # Get tasks in progress
        tasks_in_progress = await task_queue.get_tasks_in_progress_count()
        TASKS_IN_PROGRESS.set(tasks_in_progress)
        
    except Exception as e:
        # Log the error but still return metrics
        print(f"Error updating metrics: {e}")
    
    # Return Prometheus formatted metrics
    return Response(
        content=generate_latest(REGISTRY),
        media_type=CONTENT_TYPE_LATEST
    )

@router.get("/health")
async def health_check() -> Dict:
    """
    Health check endpoint for Docker healthcheck
    """
    try:
        await task_queue.redis.ping()
        return {
            "status": "healthy",
            "redis_connection": "ok",
            "api_status": "running"
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "redis_connection": "failed",
                "api_status": "running",
                "error": str(e)
            }
        )
