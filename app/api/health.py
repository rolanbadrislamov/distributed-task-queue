from fastapi import APIRouter, Depends
from typing import Dict
import aioredis

router = APIRouter()

async def check_redis_connection(redis: aioredis.Redis) -> bool:
    """Check if Redis is accessible"""
    try:
        await redis.ping()
        return True
    except Exception:
        return False

@router.get("/health")
async def health_check(redis: aioredis.Redis) -> Dict:
    """Health check endpoint"""
    redis_status = await check_redis_connection(redis)
    
    return {
        "status": "healthy" if redis_status else "unhealthy",
        "redis_connection": "ok" if redis_status else "failed",
        "api_status": "running"
    } 