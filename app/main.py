from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional

from app.core.queue import TaskQueue
from app.models.task import Task, TaskCreate, TaskResult, TaskStatus

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 