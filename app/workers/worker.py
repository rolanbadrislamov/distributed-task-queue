import asyncio
import logging
import signal
import sys
import uuid
import os
import psutil
import time
import concurrent.futures
from typing import Dict, Callable, Any, Coroutine
from datetime import datetime
from fastapi import FastAPI
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter, Gauge, REGISTRY, CollectorRegistry
from fastapi.responses import Response

from app.core.queue import TaskQueue
from app.models.task import Task, TaskStatus
from app.workers.task_handlers import fibonacci_handler, matrix_multiply_handler, sleep_handler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI()

# Initialize Prometheus metrics
REGISTRY = CollectorRegistry()
TASKS_PROCESSED = Counter('tasks_processed_total', 'Total tasks processed', registry=REGISTRY)
TASKS_FAILED = Counter('tasks_failed_total', 'Total tasks failed', registry=REGISTRY)
WORKER_CPU_USAGE = Gauge('worker_cpu_usage_percent', 'Worker CPU usage percentage', registry=REGISTRY)
WORKER_MEMORY_USAGE = Gauge('worker_memory_usage_bytes', 'Worker memory usage in bytes', registry=REGISTRY)
COMPLETED_TASKS_COUNTER = Counter('completed_tasks_total', 'Total number of completed tasks', registry=REGISTRY)
WORKER_OBJECT_COMPLETED_TASKS = Gauge('worker_object_completed_tasks', 'Gauge: Total tasks completed by the worker object instance', registry=REGISTRY)

class Worker:
    def __init__(self, queue: TaskQueue):
        self.queue = queue
        self.task_handlers: Dict[str, Callable[[Dict[str, Any]], Coroutine[Any, Any, Any]]] = {}
        self.running = False
        self.worker_id = os.getenv('WORKER_ID', str(uuid.uuid4()))
        self._setup_signal_handlers()
        self.process = psutil.Process()
        self.last_task_time = time.time()
        self.consecutive_empty_polls = 0
        self.max_batch_size = 10
        self.max_cpu_threshold = 90.0  # percent
        self.total_completed_tasks = 0  # Track total completed tasks

    def _setup_signal_handlers(self):
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._shutdown_handler)

    def _shutdown_handler(self, signum, frame):
        logger.info("Received shutdown signal, stopping worker...")
        self.running = False

    async def update_metrics(self):
        """Update worker metrics periodically"""
        while self.running:
            try:
                cpu_percent = self.process.cpu_percent()
                memory_info = self.process.memory_info()
                
                WORKER_CPU_USAGE.set(cpu_percent)
                WORKER_MEMORY_USAGE.set(memory_info.rss)
                WORKER_OBJECT_COMPLETED_TASKS.set(self.total_completed_tasks) # Add this line
                
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Error updating metrics: {str(e)}")
                await asyncio.sleep(1)

    async def send_heartbeat(self):
        """Send periodic heartbeat to indicate worker is alive"""
        while self.running:
            try:
                await self.queue.update_worker_heartbeat(self.worker_id)
                await asyncio.sleep(10)  # Send heartbeat every 10 seconds
            except Exception as e:
                logger.error(f"Error sending heartbeat: {str(e)}")
                await asyncio.sleep(1)

    def register_task_handler(self, task_type: str, handler: Callable[[Dict[str, Any]], Coroutine[Any, Any, Any]]):
        """Register a handler function for a specific task type"""
        self.task_handlers[task_type] = handler
        logger.info(f"Registered handler for task type: {task_type}")

    async def process_task(self, task: Task):
        """Process a single task"""
        logger.info(f"Processing task {task.id} of type {task.task_type}")
        
        handler = self.task_handlers.get(task.task_type)
        if not handler:
            error_msg = f"No handler registered for task type: {task.task_type}"
            logger.error(error_msg)
            TASKS_FAILED.inc()
            await self.queue.fail_task(task.id, error_msg)
            return

        try:
            # CPU overload check is now part of the main run loop to prevent fetching new tasks
            result = await handler(task.payload)
            await self.queue.complete_task(task.id, result)
            TASKS_PROCESSED.inc()
            COMPLETED_TASKS_COUNTER.inc()
            self.total_completed_tasks += 1
            logger.info(f"Task {task.id} completed successfully (Total completed: {self.total_completed_tasks})")
        except Exception as e:
            error_msg = f"Task failed: {str(e)}"
            logger.error(error_msg)
            TASKS_FAILED.inc()
            await self.queue.fail_task(task.id, error_msg)

    async def run(self, initial_poll_interval: float = 1.0):
        """Main worker loop with adaptive polling"""
        logger.info(f"Starting worker {self.worker_id}...")
        await self.queue.connect()
        self.running = True

        # Register worker and start background tasks
        await self.queue.register_worker(self.worker_id)
        heartbeat_task = asyncio.create_task(self.send_heartbeat())
        metrics_task = asyncio.create_task(self.update_metrics())

        try:
            poll_interval = initial_poll_interval
            while self.running:
                try:
                    # Check CPU load before attempting to dequeue tasks
                    cpu_percent = self.process.cpu_percent(interval=None)
                    if cpu_percent > self.max_cpu_threshold:
                        logger.warning(f"Worker {self.worker_id} CPU high ({cpu_percent}%), pausing task fetching for 1 second.")
                        await asyncio.sleep(1.0) # Pause before trying to fetch new tasks
                        continue # Skip to next iteration to re-check CPU and conditions

                    # Try to get a batch of tasks
                    tasks = []
                    for _ in range(self.max_batch_size):
                        task = await self.queue.dequeue_task()
                        if task:
                            tasks.append(task)
                        else:
                            break

                    if tasks:
                        # Process tasks concurrently
                        await asyncio.gather(*[self.process_task(task) for task in tasks])
                        self.consecutive_empty_polls = 0
                        self.last_task_time = time.time()
                        poll_interval = initial_poll_interval
                    else:
                        # Adaptive polling interval
                        self.consecutive_empty_polls += 1
                        if self.consecutive_empty_polls > 5:
                            poll_interval = min(poll_interval * 1.5, 5.0)  # Max 5 second interval
                        await asyncio.sleep(poll_interval)

                except Exception as e:
                    logger.error(f"Error in worker loop: {str(e)}")
                    await asyncio.sleep(poll_interval)
        finally:
            # Clean up
            heartbeat_task.cancel()
            metrics_task.cancel()
            try:
                await heartbeat_task
                await metrics_task
            except asyncio.CancelledError:
                pass
            logger.info("Worker stopped")
            await self.queue.disconnect()

@app.get("/metrics")
async def metrics():
    """Expose Prometheus metrics"""
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy"}

# Create global worker instance
worker = None

@app.on_event("startup")
async def startup_event():
    global worker
    queue = TaskQueue()
    worker = Worker(queue)
    worker.register_task_handler('fibonacci', fibonacci_handler)
    worker.register_task_handler('matrix_multiply', matrix_multiply_handler)
    worker.register_task_handler('sleep', sleep_handler)
    asyncio.create_task(worker.run())

@app.on_event("shutdown")
async def shutdown_event():
    global worker
    if worker:
        worker.running = False

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)