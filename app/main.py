from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, CollectorRegistry

from app.core.queue import TaskQueue
from app.api.router import create_api_router, configure_dependencies

# Initialize Prometheus metrics with a new registry
REGISTRY = CollectorRegistry()
REQUESTS = Counter('http_requests_total', 'Total HTTP requests', registry=REGISTRY)

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
    # Configure dependencies for API routers
    configure_dependencies(task_queue)

@app.on_event("shutdown")
async def shutdown_event():
    await task_queue.disconnect()

@app.middleware("http")
async def count_requests(request, call_next):
    REQUESTS.inc()
    response = await call_next(request)
    return response

# Include the API router
api_router = create_api_router()
app.include_router(api_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)