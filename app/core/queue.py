import json
from typing import Optional, List
import redis.asyncio as redis
from datetime import datetime
from app.models.task import Task, TaskStatus, TaskPriority

class TaskQueue:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.task_queue_key = "task_queue"
        self.task_hash_key = "tasks"
        self.dead_letter_queue_key = "dead_letter_queue"

    async def connect(self):
        await self.redis.ping()

    async def disconnect(self):
        await self.redis.close()

    async def enqueue_task(self, task: Task) -> str:
        # Store task details
        task_data = task.model_dump_json()
        await self.redis.hset(self.task_hash_key, task.id, task_data)

        # Add to priority queue with score based on priority and timestamp
        score = datetime.utcnow().timestamp() + (task.priority.value * 1000)
        await self.redis.zadd(self.task_queue_key, {task.id: score})
        
        return task.id

    async def dequeue_task(self) -> Optional[Task]:
        # Get task with highest priority (lowest score)
        result = await self.redis.zpopmin(self.task_queue_key)
        if not result:
            return None

        task_id, _ = result[0]
        task_data = await self.redis.hget(self.task_hash_key, task_id)
        if not task_data:
            return None

        task = Task.model_validate_json(task_data)
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.utcnow()
        await self.update_task(task)
        
        return task

    async def update_task(self, task: Task):
        task.updated_at = datetime.utcnow()
        task_data = task.model_dump_json()
        await self.redis.hset(self.task_hash_key, task.id, task_data)

    async def complete_task(self, task_id: str, result: any = None):
        task_data = await self.redis.hget(self.task_hash_key, task_id)
        if not task_data:
            return None

        task = Task.model_validate_json(task_data)
        task.status = TaskStatus.COMPLETED
        task.result = result
        task.completed_at = datetime.utcnow()
        await self.update_task(task)
        return task

    async def fail_task(self, task_id: str, error: str):
        task_data = await self.redis.hget(self.task_hash_key, task_id)
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
        task_data = await self.redis.hget(self.task_hash_key, task_id)
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
            task_data = await self.redis.hget(self.task_hash_key, task_id)
            if task_data:
                tasks.append(Task.model_validate_json(task_data))
        return tasks 