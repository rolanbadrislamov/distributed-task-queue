from enum import Enum
import time
from typing import Callable, Any, Optional
import logging

logger = logging.getLogger(__name__)

class CircuitState(Enum):
    CLOSED = "CLOSED"  # Normal operation
    OPEN = "OPEN"     # Failing state
    HALF_OPEN = "HALF_OPEN"  # Testing recovery

class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        half_open_max_trials: int = 3
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_trials = half_open_max_trials
        
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
        self.half_open_trials = 0

    def can_execute(self) -> bool:
        """Check if the circuit breaker allows execution"""
        if self.state == CircuitState.CLOSED:
            return True
        
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                self.half_open_trials = 0
                logger.info("Circuit switched to HALF_OPEN state")
                return True
            return False
            
        if self.state == CircuitState.HALF_OPEN:
            return self.half_open_trials < self.half_open_max_trials

        return False

    def record_success(self):
        """Record a successful execution"""
        if self.state == CircuitState.HALF_OPEN:
            self.half_open_trials += 1
            if self.half_open_trials >= self.half_open_max_trials:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.last_failure_time = None
                logger.info("Circuit switched back to CLOSED state")
        elif self.state == CircuitState.CLOSED:
            self.failure_count = 0

    def record_failure(self):
        """Record a failed execution"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.state == CircuitState.HALF_OPEN or \
           (self.state == CircuitState.CLOSED and self.failure_count >= self.failure_threshold):
            self.state = CircuitState.OPEN
            logger.warning("Circuit switched to OPEN state")

    async def execute(self, func: Callable, *args, **kwargs) -> Optional[Any]:
        """Execute a function with circuit breaker protection"""
        if not self.can_execute():
            logger.warning("Circuit breaker prevented execution")
            return None

        try:
            result = await func(*args, **kwargs)
            self.record_success()
            return result
        except Exception as e:
            self.record_failure()
            logger.error(f"Circuit breaker caught error: {str(e)}")
            raise 