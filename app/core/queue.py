import json
import os
from typing import Optional, List
import aioredis
from datetime import datetime
from app.models.task import Task, TaskStatus, TaskPriority

class TaskQueue:
    def __init__(self):
        self.redis = None
        self.task_queue_key = "task_queue"
        self.task_hash_key = "tasks"
        self.dead_letter_queue_key = "dead_letter_queue"
        self.active_workers_key = "active_workers"
        self.tasks_in_progress_key = "tasks_in_progress"
        
        # Support multiple Redis URL formats - match your docker-compose env vars
        redis_host = os.getenv('REDIS_HOST', 'redis')  # Match service name in docker-compose
        redis_port = os.getenv('REDIS_PORT', '6379')
        self.redis_url = f'redis://{redis_host}:{redis_port}'

    async def connect(self):
        try:
            # Try different connection methods based on aioredis version
            try:
                # For aioredis 2.x
                self.redis = aioredis.from_url(
                    self.redis_url,
                    encoding='utf-8',
                    decode_responses=True
                )
            except AttributeError:
                # For older aioredis versions
                self.redis = await aioredis.create_redis_pool(
                    self.redis_url,
                    encoding='utf-8'
                )
            
            # Test the connection
            await self.redis.ping()
            print(f"Successfully connected to Redis at {self.redis_url}")
            
        except Exception as e:
            print(f"Failed to connect to Redis at {self.redis_url}: {str(e)}")
            print("Make sure Redis is running and accessible")
            raise

    async def disconnect(self):
        if self.redis is not None:
            if hasattr(self.redis, 'close'):
                await self.redis.close()
            else:
                self.redis.close()
                await self.redis.wait_closed()

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
        result = await self.redis.zpopmin(self.task_queue_key, 1)
        if not result:
            return None

        # Handle different aioredis return formats
        if isinstance(result, list) and result:
            task_id = result[0][0] if isinstance(result[0][0], str) else result[0][0].decode('utf-8')
        else:
            task_id = list(result.keys())[0] if result else None
            
        if not task_id:
            return None

        task_data = await self.redis.hget(self.task_hash_key, task_id)
        if not task_data:
            return None

        task = Task.model_validate_json(task_data)
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.utcnow()
        
        # Add task to in-progress set
        await self.redis.sadd(self.tasks_in_progress_key, task_id)
        await self.update_task(task)
        
        return task

    async def update_task(self, task: Task):
        task.updated_at = datetime.utcnow()
        task_data = task.model_dump_json()
        await self.redis.hset(self.task_hash_key, task.id, task_data)

    async def complete_task(self, task_id: str, result: any = None):
        # Remove task from in-progress set
        await self.redis.srem(self.tasks_in_progress_key, task_id)
        
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
        # Remove task from in-progress set
        await self.redis.srem(self.tasks_in_progress_key, task_id)
        
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
            task_id_str = task_id if isinstance(task_id, str) else task_id.decode('utf-8')
            task_data = await self.redis.hget(self.task_hash_key, task_id_str)
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