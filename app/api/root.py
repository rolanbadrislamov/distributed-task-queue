from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter(tags=["root"])

@router.get("/")
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
            "health": "/health"
        }
    })
