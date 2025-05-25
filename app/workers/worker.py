import asyncio
import logging
import signal
import sys
from typing import Dict, Callable, Any, Coroutine
from datetime import datetime

from app.core.queue import TaskQueue
from app.models.task import Task, TaskStatus
from app.workers.example_tasks import register_example_handlers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Worker:
    def __init__(self, queue: TaskQueue):
        self.queue = queue
        self.task_handlers: Dict[str, Callable[[Dict[str, Any]], Coroutine[Any, Any, Any]]] = {}
        self.running = False
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._shutdown_handler)

    def _shutdown_handler(self, signum, frame):
        logger.info("Received shutdown signal, stopping worker...")
        self.running = False

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
            await self.queue.fail_task(task.id, error_msg)
            return

        try:
            result = await handler(task.payload)
            await self.queue.complete_task(task.id, result)
            logger.info(f"Task {task.id} completed successfully")
        except Exception as e:
            error_msg = f"Task failed: {str(e)}"
            logger.error(error_msg)
            await self.queue.fail_task(task.id, error_msg)

    async def run(self, poll_interval: float = 1.0):
        """Main worker loop"""
        logger.info("Starting worker...")
        await self.queue.connect()
        self.running = True

        while self.running:
            try:
                task = await self.queue.dequeue_task()
                if task:
                    await self.process_task(task)
                else:
                    await asyncio.sleep(poll_interval)
            except Exception as e:
                logger.error(f"Error in worker loop: {str(e)}")
                await asyncio.sleep(poll_interval)

        logger.info("Worker stopped")
        await self.queue.disconnect()

# Example task handlers
async def example_task_handler(payload: Dict[str, Any]) -> Any:
    """Example task handler that simulates some work"""
    logger.info(f"Processing example task with payload: {payload}")
    # Simulate some work
    await asyncio.sleep(2)
    return {"result": "Task completed", "input": payload}

async def main():
    # Create queue and worker instances
    queue = TaskQueue()
    worker = Worker(queue)

    # Register example task handlers
    register_example_handlers(worker)

    # Run the worker
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main()) 