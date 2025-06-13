import time
import psutil
import asyncio
from typing import Dict, List
import logging
from prometheus_client import Counter, Gauge, Histogram
import aioredis

logger = logging.getLogger(__name__)

# Prometheus metrics
TASK_COUNTER = Counter('task_total', 'Total number of tasks processed')
QUEUE_SIZE = Gauge('queue_size', 'Current size of the task queue')
WORKER_LOAD = Gauge('worker_load', 'Current worker CPU load')
MEMORY_USAGE = Gauge('memory_usage_bytes', 'Current memory usage')
ACTIVE_WORKERS = Gauge('active_workers', 'Number of active workers')

class MetricsCollector:
    def __init__(
        self,
        redis_client: aioredis.Redis,
        collection_interval: int = 5
    ):
        self.redis = redis_client
        self.collection_interval = collection_interval
        self._collection_task = None
        self.metrics_history: List[Dict] = []
        self.max_history_size = 1000

    async def start(self):
        """Start metrics collection"""
        self._collection_task = asyncio.create_task(self._collect_metrics_loop())
        logger.info("Metrics collector started")

    async def stop(self):
        """Stop metrics collection"""
        if self._collection_task:
            self._collection_task.cancel()
            try:
                await self._collection_task
            except asyncio.CancelledError:
                pass
        logger.info("Metrics collector stopped")

    async def _collect_metrics_loop(self):
        """Continuously collect metrics"""
        while True:
            try:
                metrics = await self._collect_metrics()
                self._update_prometheus_metrics(metrics)
                self._store_metrics_history(metrics)
                await asyncio.sleep(self.collection_interval)
            except Exception as e:
                logger.error(f"Error collecting metrics: {str(e)}")
                await asyncio.sleep(self.collection_interval)

    async def _collect_metrics(self) -> Dict:
        """Collect current system metrics"""
        # System metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        
        # Redis metrics
        queue_size = await self.redis.llen("task_queue")
        active_workers = await self.redis.scard("active_workers")
        processing_tasks = await self.redis.scard("processing_tasks")
        
        # Calculate task throughput
        current_time = time.time()
        completed_tasks = await self.redis.zcount(
            "completed_tasks",
            current_time - 60,  # Last minute
            current_time
        )

        metrics = {
            "timestamp": current_time,
            "system": {
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "memory_used": memory.used,
                "memory_total": memory.total
            },
            "queue": {
                "size": queue_size,
                "active_workers": active_workers,
                "processing_tasks": processing_tasks,
                "completed_tasks_per_minute": completed_tasks
            }
        }

        return metrics

    def _update_prometheus_metrics(self, metrics: Dict):
        """Update Prometheus metrics"""
        QUEUE_SIZE.set(metrics["queue"]["size"])
        WORKER_LOAD.set(metrics["system"]["cpu_percent"])
        MEMORY_USAGE.set(metrics["system"]["memory_used"])
        ACTIVE_WORKERS.set(metrics["queue"]["active_workers"])

    def _store_metrics_history(self, metrics: Dict):
        """Store metrics history"""
        self.metrics_history.append(metrics)
        
        # Maintain history size limit
        if len(self.metrics_history) > self.max_history_size:
            self.metrics_history = self.metrics_history[-self.max_history_size:]

    def get_metrics_summary(self) -> Dict:
        """Get a summary of current metrics"""
        if not self.metrics_history:
            return {}

        latest = self.metrics_history[-1]
        
        # Calculate averages over the last minute
        minute_ago = time.time() - 60
        recent_metrics = [m for m in self.metrics_history 
                         if m["timestamp"] > minute_ago]
        
        if recent_metrics:
            avg_queue_size = sum(m["queue"]["size"] for m in recent_metrics) / len(recent_metrics)
            avg_cpu = sum(m["system"]["cpu_percent"] for m in recent_metrics) / len(recent_metrics)
        else:
            avg_queue_size = latest["queue"]["size"]
            avg_cpu = latest["system"]["cpu_percent"]

        return {
            "current": latest,
            "averages": {
                "queue_size": avg_queue_size,
                "cpu_percent": avg_cpu
            },
            "active_workers": latest["queue"]["active_workers"],
            "processing_tasks": latest["queue"]["processing_tasks"]
        } 