import asyncio
from typing import Optional
import time
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class SystemMetrics:
    queue_size: int
    worker_load: float
    memory_usage: float

class BackPressure:
    def __init__(
        self,
        max_queue_size: int = 5000,
        max_worker_load: float = 0.98,
        max_memory_usage: float = 0.98,
        cooldown_period: int = 10
    ):
        self.max_queue_size = max_queue_size
        self.max_worker_load = max_worker_load
        self.max_memory_usage = max_memory_usage
        self.cooldown_period = cooldown_period
        self.last_throttle_time = 0
        self.current_delay = 0

    async def should_throttle(self, metrics: SystemMetrics) -> bool:
        """Determine if the system should apply back pressure"""
        if any([
            metrics.queue_size >= self.max_queue_size,
            metrics.worker_load >= self.max_worker_load,
            metrics.memory_usage >= self.max_memory_usage
        ]):
            return True
        return False

    def calculate_delay(self, metrics: SystemMetrics) -> float:
        """Calculate delay based on system load"""
        queue_factor = metrics.queue_size / self.max_queue_size
        worker_factor = metrics.worker_load / self.max_worker_load
        memory_factor = metrics.memory_usage / self.max_memory_usage
        
        # Use the highest load factor to determine delay
        load_factor = max(queue_factor, worker_factor, memory_factor)
        
        # Less aggressive backoff: max 2.5s, starting lower
        return min(0.5 + load_factor, 2.5)

    async def apply_back_pressure(self, metrics: SystemMetrics) -> Optional[float]:
        """Apply back pressure if needed"""
        if not await self.should_throttle(metrics):
            self.current_delay = 0
            return None

        current_time = time.time()
        if current_time - self.last_throttle_time >= self.cooldown_period:
            self.current_delay = 0

        self.current_delay = self.calculate_delay(metrics)
        self.last_throttle_time = current_time

        logger.warning(f"Applying back pressure with delay: {self.current_delay}s")
        return self.current_delay

    async def handle_request(self, metrics: SystemMetrics, handler_func, *args, **kwargs):
        """Handle a request with back pressure"""
        delay = await self.apply_back_pressure(metrics)
        
        if delay:
            logger.info(f"Back pressure: Delaying request by {delay}s")
            await asyncio.sleep(delay)
        
        return await handler_func(*args, **kwargs)

    def get_current_status(self) -> dict:
        """Get current back pressure status"""
        return {
            "is_throttling": self.current_delay > 0,
            "current_delay": self.current_delay,
            "time_since_last_throttle": time.time() - self.last_throttle_time
        } 