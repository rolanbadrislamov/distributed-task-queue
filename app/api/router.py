from fastapi import APIRouter
from app.api import tasks, monitoring, root

def create_api_router() -> APIRouter:
    """
    Create and configure the main API router with all sub-routers
    """
    api_router = APIRouter()
    
    # Include all sub-routers
    api_router.include_router(root.router)
    api_router.include_router(tasks.router)
    api_router.include_router(monitoring.router)
    
    return api_router

def configure_dependencies(task_queue):
    """
    Configure dependencies for all API routers
    """
    tasks.set_task_queue(task_queue)
    monitoring.set_task_queue(task_queue)
