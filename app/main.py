from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional, Dict
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter, Gauge, REGISTRY, CollectorRegistry
from fastapi.responses import Response, JSONResponse
import psutil
import os

from app.core.queue import TaskQueue
from app.models.task import Task, TaskCreate, TaskResult, TaskStatus

# Initialize Prometheus metrics with a new registry
REGISTRY = CollectorRegistry()
REQUESTS = Counter('http_requests_total', 'Total HTTP requests', registry=REGISTRY)
TASKS_CREATED = Counter('tasks_created_total', 'Total tasks created', registry=REGISTRY)
QUEUE_SIZE = Gauge('queue_size', 'Current size of task queue', registry=REGISTRY)
DEAD_LETTER_QUEUE_SIZE = Gauge('dead_letter_queue_size', 'Size of dead letter queue', registry=REGISTRY)

# New metrics for worker stats
ACTIVE_WORKERS = Gauge('active_workers', 'Number of active workers', registry=REGISTRY)
WORKER_MEMORY_USAGE = Gauge('worker_memory_usage_bytes', 'Worker memory usage in bytes', registry=REGISTRY)
WORKER_CPU_LOAD = Gauge('worker_cpu_load_percent', 'Worker CPU load percentage', registry=REGISTRY)
TASKS_IN_PROGRESS = Gauge('tasks_in_progress', 'Number of tasks currently being processed', registry=REGISTRY)

app = FastAPI(
    title="Distributed Task Queue",
    description="A scalable distributed task queue system",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create global task queue instance
task_queue = TaskQueue()

@app.on_event("startup")
async def startup_event():
    await task_queue.connect()

@app.on_event("shutdown")
async def shutdown_event():
    await task_queue.disconnect()

@app.middleware("http")
async def count_requests(request, call_next):
    REQUESTS.inc()
    response = await call_next(request)
    return response

@app.post("/tasks/", response_model=Task)
async def create_task(task_create: TaskCreate):
    """
    Create a new task and add it to the queue.
    """
    task = Task(
        task_type=task_create.task_type,
        payload=task_create.payload,
        priority=task_create.priority,
        max_retries=task_create.max_retries
    )
    await task_queue.enqueue_task(task)
    TASKS_CREATED.inc()
    return task

@app.get("/tasks/{task_id}", response_model=TaskResult)
async def get_task_status(task_id: str):
    """
    Get the status and result of a specific task.
    """
    task = await task_queue.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return TaskResult(
        task_id=task.id,
        status=task.status,
        result=task.result,
        error=task.error
    )

@app.get("/tasks/", response_model=dict)
async def get_queue_stats():
    """
    Get statistics about the task queue.
    """
    queue_length = await task_queue.get_queue_length()
    dead_letter_queue_length = await task_queue.get_dead_letter_queue_length()
    
    # Update Prometheus metrics
    QUEUE_SIZE.set(queue_length)
    DEAD_LETTER_QUEUE_SIZE.set(dead_letter_queue_length)
    
    return {
        "queue_length": queue_length,
        "dead_letter_queue_length": dead_letter_queue_length
    }

@app.get("/tasks/failed/", response_model=List[Task])
async def get_failed_tasks(limit: Optional[int] = 10):
    """
    Get a list of failed tasks from the dead letter queue.
    """
    return await task_queue.get_failed_tasks(limit=limit)

@app.get("/metrics")
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

@app.get("/metrics/detailed")
async def detailed_metrics():
    """
    Get detailed metrics about the task queue and workers in JSON format
    """
    # Get queue statistics
    queue_length = await task_queue.get_queue_length()
    dead_letter_queue_length = await task_queue.get_dead_letter_queue_length()
    
    # Get process metrics
    process = psutil.Process(os.getpid())
    memory_usage = process.memory_info().rss
    cpu_percent = process.cpu_percent()
    
    # Get active workers count from Redis
    active_workers = await task_queue.get_active_workers_count()
    
    # Get tasks in progress
    tasks_in_progress = await task_queue.get_tasks_in_progress_count()
    
    return {
        "queue_stats": {
            "queue_length": queue_length,
            "dead_letter_queue_length": dead_letter_queue_length,
            "tasks_in_progress": tasks_in_progress
        },
        "worker_stats": {
            "active_workers": active_workers,
            "memory_usage_bytes": memory_usage,
            "cpu_load_percent": cpu_percent
        }
    }

@app.get("/health")
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

@app.get("/")
async def root():
    """
    Root endpoint that provides API information.
    """
    return JSONResponse({
        "name": "Distributed Task Queue API",
        "version": "1.0.0",
        "description": "A scalable distributed task queue system",
        "endpoints": {
            "tasks": "/tasks/",
            "task_status": "/tasks/{task_id}",
            "queue_stats": "/tasks/",
            "failed_tasks": "/tasks/failed/",
            "metrics": "/metrics",
            "detailed_metrics": "/metrics/detailed",
            "health": "/health"
        }
    })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)