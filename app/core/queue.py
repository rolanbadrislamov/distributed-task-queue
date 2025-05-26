import json
import os
from typing import Optional, List
import aioredis
from datetime import datetime
from app.models.task import Task, TaskStatus, TaskPriority

class TaskQueue:
    _redis_pool = None  # Class level connection pool

    def __init__(self):
        self.redis = None
        self.task_queue_key = "task_queue"
        self.task_storage_key = "task_storage"
        self.dead_letter_queue_key = "dead_letter_queue"
        self.active_workers_key = "active_workers"
        self.tasks_in_progress_key = "tasks_in_progress"

    @classmethod
    async def create_pool(cls):
        """Create a Redis connection pool if it doesn't exist"""
        if cls._redis_pool is None:
            redis_url = os.getenv("REDIS_URL", "redis://localhost")
            cls._redis_pool = await aioredis.create_redis_pool(
                redis_url,
                minsize=10,
                maxsize=50,
                encoding='utf-8'
            )

    async def connect(self):
        """Get a connection from the pool"""
        await self.__class__.create_pool()
        if not self.redis:
            self.redis = self._redis_pool

    async def disconnect(self):
        """Return connection to pool"""
        self.redis = None

    async def enqueue_task(self, task: Task) -> str:
        # Store task details
        task_data = task.model_dump_json()
        await self.redis.hset(self.task_storage_key, task.id, task_data)

        # Add to priority queue with score based on priority and timestamp
        score = datetime.utcnow().timestamp() + (task.priority.value * 1000)
        await self.redis.zadd(self.task_queue_key, {task.id: score})
        
        return task.id

    async def dequeue_task(self) -> Optional[Task]:
        """Dequeue a task with optimized locking"""
        try:
            # Use Redis transaction to ensure atomic operations
            tr = self.redis.multi_exec()
            # Get task ID from queue
            tr.rpop(self.task_queue_key)
            task_id = await tr.execute()
            
            if not task_id or not task_id[0]:
                return None
                
            task_id = task_id[0]
            # Get task data
            task_data = await self.redis.hget(self.task_storage_key, task_id)
            if not task_data:
                return None

            task = Task.parse_raw(task_data)
            # Mark task as in progress
            await self.redis.sadd(self.tasks_in_progress_key, task_id)
            return task

        except Exception as e:
            logger.error(f"Error dequeuing task: {str(e)}")
            return None

    async def update_task(self, task: Task):
        task.updated_at = datetime.utcnow()
        task_data = task.model_dump_json()
        await self.redis.hset(self.task_storage_key, task.id, task_data)

    async def complete_task(self, task_id: str, result: any = None):
        # Remove task from in-progress set
        await self.redis.srem(self.tasks_in_progress_key, task_id)
        
        task_data = await self.redis.hget(self.task_storage_key, task_id)
        if not task_data:
            return None

        task = Task.model_validate_json(task_data)
        task.status = TaskStatus.COMPLETED
        task.result = result
        task.completed_at = datetime.utcnow()
        await self.update_task(task)
        return task

    async def fail_task(self, task_id: str, error: str):
        # Remove task from in-progress set
        await self.redis.srem(self.tasks_in_progress_key, task_id)
        
        task_data = await self.redis.hget(self.task_storage_key, task_id)
        if not task_data:
            return None

        task = Task.model_validate_json(task_data)
        task.retry_count += 1

        if task.retry_count >= task.max_retries:
            task.status = TaskStatus.FAILED
            task.error = error
            # Move to dead letter queue
            await self.redis.zadd(self.dead_letter_queue_key, {task_id: datetime.utcnow().timestamp()})
        else:
            task.status = TaskStatus.RETRY
            # Requeue with backoff
            backoff = 2 ** task.retry_count
            score = datetime.utcnow().timestamp() + backoff
            await self.redis.zadd(self.task_queue_key, {task_id: score})

        await self.update_task(task)
        return task

    async def get_task(self, task_id: str) -> Optional[Task]:
        task_data = await self.redis.hget(self.task_storage_key, task_id)
        if not task_data:
            return None
        return Task.model_validate_json(task_data)

    async def get_queue_length(self) -> int:
        return await self.redis.zcard(self.task_queue_key)

    async def get_dead_letter_queue_length(self) -> int:
        return await self.redis.zcard(self.dead_letter_queue_key)

    async def get_failed_tasks(self, limit: int = 10) -> List[Task]:
        task_ids = await self.redis.zrange(self.dead_letter_queue_key, 0, limit - 1)
        tasks = []
        for task_id in task_ids:
            task_id_str = task_id if isinstance(task_id, str) else task_id.decode('utf-8')
            task_data = await self.redis.hget(self.task_storage_key, task_id_str)
            if task_data:
                tasks.append(Task.model_validate_json(task_data))
        return tasks

    async def get_active_workers_count(self) -> int:
        """Get the number of active workers"""
        # Clean up stale workers (older than 30 seconds)
        current_time = datetime.utcnow().timestamp()
        await self.redis.zremrangebyscore(self.active_workers_key, 0, current_time - 30)
        return await self.redis.zcard(self.active_workers_key)

    async def get_tasks_in_progress_count(self) -> int:
        """Get the number of tasks currently being processed"""
        return await self.redis.scard(self.tasks_in_progress_key)

    async def register_worker(self, worker_id: str):
        """Register a worker as active"""
        current_time = datetime.utcnow().timestamp()
        await self.redis.zadd(self.active_workers_key, {worker_id: current_time})

    async def update_worker_heartbeat(self, worker_id: str):
        """Update worker heartbeat timestamp"""
        current_time = datetime.utcnow().timestamp()
        await self.redis.zadd(self.active_workers_key, {worker_id: current_time})