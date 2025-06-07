import json
import os
import logging
from typing import Optional, List
import redis.asyncio as aioredis
from datetime import datetime
from app.models.task import Task, TaskStatus, TaskPriority
import asyncio

logger = logging.getLogger(__name__)

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
            redis_host = os.getenv("REDIS_HOST", "redis")
            redis_port = os.getenv("REDIS_PORT", "6379")
            redis_url = os.getenv("REDIS_URL", f"redis://{redis_host}:{redis_port}")
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    cls._redis_pool = await aioredis.from_url(
                        redis_url,
                        encoding='utf-8',
                        decode_responses=True,
                        max_connections=250
                    )
                    # Test connection
                    await cls._redis_pool.ping()
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(2 ** attempt)

    async def connect(self):
        await self.__class__.create_pool()
        if not self.redis:
            self.redis = self._redis_pool
            # Health check
            for _ in range(3):
                try:
                    await self.redis.ping()
                    break
                except Exception:
                    await asyncio.sleep(1)

    async def disconnect(self):
        """Return connection to pool"""
        self.redis = None

    _enqueue_lua = """
    -- KEYS[1]: task_storage_key
    -- KEYS[2]: task_queue_key
    -- ARGV[1]: task.id
    -- ARGV[2]: task_data (JSON string)
    -- ARGV[3]: score (for the sorted set)

    redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
    redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
    return ARGV[1]
    """

    async def enqueue_task(self, task: Task) -> str:
        task_data = task.model_dump_json()
        score = datetime.utcnow().timestamp() + (task.priority.value * 1000)
        
        try:
            task_id = await self.redis.eval(
                self._enqueue_lua,
                2,
                self.task_storage_key,
                self.task_queue_key,
                task.id,
                task_data,
                score
            )
            return task_id
        except aioredis.exceptions.ResponseError as e:
            logger.error(f"Lua script error or Redis connection issue during enqueue: {str(e)}")
            # Fallback or re-raise, depending on desired error handling
            raise
        except Exception as e:
            logger.error(f"Error enqueuing task: {str(e)}")
            raise

    # Atomic Lua script for dequeueing and marking in-progress
    _dequeue_lua = """
    local task_id = redis.call('zpopmax', KEYS[1])
    if not task_id[1] then return nil end
    
    local task_data = redis.call('hget', KEYS[3], task_id[1])
    if not task_data then
        -- Task data not found, potentially an orphaned ID. Remove from in-progress if added.
        -- This case should be rare if enqueue and storage are consistent.
        return nil 
    end
    
    redis.call('sadd', KEYS[2], task_id[1])
    return {task_id[1], task_data}
    """

    async def dequeue_task(self) -> Optional[Task]:
        try:
            # EVALSHA can be used if the script is loaded once with SCRIPT LOAD
            # For simplicity, using EVAL directly here.
            result = await self.redis.eval(
                self._dequeue_lua, 
                3, 
                self.task_queue_key, 
                self.tasks_in_progress_key,
                self.task_storage_key
            )
            if not result:
                return None
            
            task_id, task_data_str = result
            if not task_data_str: # Should be caught by Lua, but as a safeguard
                logger.warning(f"Dequeued task ID {task_id} but no data found in storage.")
                await self.redis.srem(self.tasks_in_progress_key, task_id) # Clean up
                return None

            task = Task.model_validate_json(task_data_str)
            return task
        except aioredis.exceptions.ResponseError as e:
            # Handle potential script errors or connection issues
            logger.error(f"Lua script error or Redis connection issue during dequeue: {str(e)}")
            return None
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