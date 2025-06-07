from fastapi import APIRouter, HTTPException
from typing import List, Optional
from prometheus_client import Counter

from app.core.queue import TaskQueue
from app.models.task import Task, TaskCreate, TaskResult

# Prometheus metrics for tasks
TASKS_CREATED = Counter('tasks_created_total', 'Total tasks created')

router = APIRouter(prefix="/tasks", tags=["tasks"])

# This will be injected as dependency
task_queue: TaskQueue = None

def set_task_queue(queue: TaskQueue):
    """Set the task queue instance for this router"""
    global task_queue
    task_queue = queue

@router.post("/", response_model=Task)
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

@router.get("/{task_id}", response_model=TaskResult)
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

@router.get("/", response_model=dict)
async def get_queue_stats():
    """
    Get statistics about the task queue.
    """
    queue_length = await task_queue.get_queue_length()
    dead_letter_queue_length = await task_queue.get_dead_letter_queue_length()
    
    return {
        "queue_length": queue_length,
        "dead_letter_queue_length": dead_letter_queue_length
    }

@router.get("/failed/", response_model=List[Task])
async def get_failed_tasks(limit: Optional[int] = 10):
    """
    Get a list of failed tasks from the dead letter queue.
    """
    return await task_queue.get_failed_tasks(limit=limit)
