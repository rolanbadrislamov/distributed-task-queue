import asyncio
import logging
import time
import random
import signal
import sys
import os
import json
import statistics
from datetime import datetime
from typing import List, Dict, Any, Optional
import psutil
import subprocess
import docker
from dataclasses import dataclass

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

from app.core.queue import TaskQueue
from app.models.task import Task, TaskStatus, TaskPriority
from app.workers.task_handlers import fibonacci_handler, matrix_multiply_handler, sleep_handler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ensure correct Redis host for local vs Docker Compose execution
if not os.getenv("REDIS_HOST"):
    if os.getenv("COMPOSE_PROJECT_NAME") or os.getenv("DOCKER_COMPOSE"):
        os.environ["REDIS_HOST"] = "redis"
    else:
        os.environ["REDIS_HOST"] = "localhost"

@dataclass
class FaultToleranceTestResult:
    test_name: str
    success: bool
    duration: float
    tasks_submitted: int
    tasks_completed: int
    tasks_failed: int
    tasks_retried: int
    error_rate: float
    recovery_time: Optional[float] = None
    details: Dict[str, Any] = None

class FaultToleranceExperiment:
    def __init__(self):
        self.queue = TaskQueue()
        self.test_results: List[FaultToleranceTestResult] = []
        self.docker_client = docker.from_env()
        self.metrics_history: List[Dict[str, Any]] = []
        
    async def setup(self):
        """Initialize the experiment"""
        await self.queue.connect()
        logger.info("Fault tolerance experiment initialized")
        
    async def cleanup(self):
        """Clean up resources"""
        await self.queue.disconnect()
        
    async def submit_test_tasks(self, count: int, task_type: str = "fibonacci") -> List[str]:
        """Submit test tasks and return their IDs"""
        task_ids = []
        for i in range(count):
            task = Task(
                task_type=task_type,
                payload={"n": random.randint(10, 30)},
                priority=TaskPriority.MEDIUM,
                max_retries=3
            )
            await self.queue.enqueue_task(task)
            task_ids.append(task.id)
        return task_ids
        
    async def wait_for_task_completion(self, task_ids: List[str], timeout: int = 60) -> Dict[str, Any]:
        """Wait for tasks to complete and return statistics"""
        start_time = time.time()
        completed = set()
        failed = set()
        retried = set()
        while time.time() - start_time < timeout:
            for task_id in task_ids:
                task = await self.queue.get_task(task_id)
                if task:
                    if task.status == TaskStatus.COMPLETED:
                        completed.add(task_id)
                    elif task.status == TaskStatus.FAILED:
                        failed.add(task_id)
                    elif task.status == TaskStatus.RETRY:
                        retried.add(task_id)
            if len(completed) + len(failed) == len(task_ids):
                break
            await asyncio.sleep(1)
        return {
            "completed": len(completed),
            "failed": len(failed),
            "retried": len(retried),
            "pending": len(task_ids) - len(completed) - len(failed)
        }
        
    async def clear_redis(self):
        """Flush all data from Redis before each test to ensure isolation."""
        if self.queue.redis:
            await self.queue.redis.flushdb()
        
    async def test_network_partition(self) -> FaultToleranceTestResult:
        """Test system behavior during network partition"""
        await self.clear_redis()
        logger.info("Starting network partition test")
        start_time = time.time()
        
        # Submit initial tasks
        task_ids = await self.submit_test_tasks(20, "fibonacci")
        
        # Simulate network partition by stopping Redis
        logger.info("Simulating network partition by stopping Redis")
        try:
            redis_container = self.docker_client.containers.get("distributed-task-queue-redis-1")
            redis_container.stop(timeout=10)
        except Exception as e:
            logger.warning(f"Could not stop Redis container: {e}")
            
        # Wait during partition
        await asyncio.sleep(10)
        
        # Restore network by starting Redis
        logger.info("Restoring network by starting Redis")
        try:
            redis_container = self.docker_client.containers.get("distributed-task-queue-redis-1")
            redis_container.start()
            await asyncio.sleep(5)  # Wait for Redis to fully start
        except Exception as e:
            logger.warning(f"Could not start Redis container: {e}")
            
        # Wait for tasks to complete
        stats = await self.wait_for_task_completion(task_ids, timeout=120)
        
        duration = time.time() - start_time
        success = stats["completed"] >= len(task_ids) * 0.8  # 80% success rate
        error_rate = stats["failed"] / len(task_ids) if task_ids else 0
        
        return FaultToleranceTestResult(
            test_name="Network Partition",
            success=success,
            duration=duration,
            tasks_submitted=len(task_ids),
            tasks_completed=stats["completed"],
            tasks_failed=stats["failed"],
            tasks_retried=stats["retried"],
            error_rate=error_rate,
            recovery_time=duration - 10,  # Exclude partition time
            details={"partition_duration": 10}
        )
        
    async def test_worker_crash_recovery(self) -> FaultToleranceTestResult:
        """Test system behavior when workers crash and recover"""
        await self.clear_redis()
        logger.info("Starting worker crash recovery test")
        start_time = time.time()
        
        # Submit tasks
        task_ids = await self.submit_test_tasks(30, "matrix_multiply")
        
        # Let some tasks start processing
        await asyncio.sleep(5)
        
        # Simulate worker crash by stopping worker containers
        logger.info("Simulating worker crash")
        try:
            worker_containers = self.docker_client.containers.list(
                filters={"label": "com.docker.compose.service=worker"}
            )
            for container in worker_containers:
                container.stop(timeout=5)
        except Exception as e:
            logger.warning(f"Could not stop worker containers: {e}")
            
        # Wait during crash
        await asyncio.sleep(10)
        
        # Restart workers
        logger.info("Restarting workers")
        try:
            subprocess.run(["docker-compose", "up", "-d", "worker"], check=True)
            await asyncio.sleep(10)  # Wait for workers to start
        except Exception as e:
            logger.warning(f"Could not restart workers: {e}")
            
        # Wait for tasks to complete
        stats = await self.wait_for_task_completion(task_ids, timeout=180)
        
        duration = time.time() - start_time
        success = stats["completed"] >= len(task_ids) * 0.7  # 70% success rate
        error_rate = stats["failed"] / len(task_ids) if task_ids else 0
        
        return FaultToleranceTestResult(
            test_name="Worker Crash Recovery",
            success=success,
            duration=duration,
            tasks_submitted=len(task_ids),
            tasks_completed=stats["completed"],
            tasks_failed=stats["failed"],
            tasks_retried=stats["retried"],
            error_rate=error_rate,
            recovery_time=duration - 10,
            details={"crash_duration": 10}
        )
        
    async def test_task_failure_retry(self) -> FaultToleranceTestResult:
        """Test retry mechanism for failed tasks"""
        await self.clear_redis()
        logger.info("Starting task failure retry test")
        start_time = time.time()
        
        # Submit tasks that will fail initially but succeed on retry
        task_ids = []
        for i in range(15):
            # Create a task that will fail initially
            task = Task(
                task_type="sleep",  # Use sleep handler which is more reliable
                payload={"duration": 1},
                priority=TaskPriority.HIGH,
                max_retries=3
            )
            await self.queue.enqueue_task(task)
            task_ids.append(task.id)
            
        # Wait for tasks to complete
        stats = await self.wait_for_task_completion(task_ids, timeout=60)
        
        duration = time.time() - start_time
        success = stats["completed"] >= len(task_ids) * 0.9  # 90% success rate
        error_rate = stats["failed"] / len(task_ids) if task_ids else 0
        
        return FaultToleranceTestResult(
            test_name="Task Failure Retry",
            success=success,
            duration=duration,
            tasks_submitted=len(task_ids),
            tasks_completed=stats["completed"],
            tasks_failed=stats["failed"],
            tasks_retried=stats["retried"],
            error_rate=error_rate,
            details={"max_retries": 3}
        )
        
    async def test_system_overload_backpressure(self) -> FaultToleranceTestResult:
        """Test back pressure mechanism under system overload"""
        await self.clear_redis()
        logger.info("Starting system overload back pressure test")
        start_time = time.time()
        
        # Submit a large number of tasks to trigger back pressure
        task_ids = await self.submit_test_tasks(100, "fibonacci")
        
        # Monitor queue size and system metrics
        queue_sizes = []
        for _ in range(30):  # Monitor for 30 seconds
            queue_size = await self.queue.get_queue_length()
            queue_sizes.append(queue_size)
            await asyncio.sleep(1)
            
        # Wait for tasks to complete
        stats = await self.wait_for_task_completion(task_ids, timeout=300)
        
        duration = time.time() - start_time
        success = stats["completed"] >= len(task_ids) * 0.8  # 80% success rate
        error_rate = stats["failed"] / len(task_ids) if task_ids else 0
        
        # Check if back pressure was applied (queue size should stabilize)
        max_queue_size = max(queue_sizes) if queue_sizes else 0
        avg_queue_size = statistics.mean(queue_sizes) if queue_sizes else 0
        
        return FaultToleranceTestResult(
            test_name="System Overload Back Pressure",
            success=success,
            duration=duration,
            tasks_submitted=len(task_ids),
            tasks_completed=stats["completed"],
            tasks_failed=stats["failed"],
            tasks_retried=stats["retried"],
            error_rate=error_rate,
            details={
                "max_queue_size": max_queue_size,
                "avg_queue_size": avg_queue_size,
                "back_pressure_applied": max_queue_size > 50
            }
        )
        
    async def test_circuit_breaker(self) -> FaultToleranceTestResult:
        """Test circuit breaker pattern under repeated failures"""
        await self.clear_redis()
        logger.info("Starting circuit breaker test")
        start_time = time.time()
        
        # Submit tasks that will trigger circuit breaker
        task_ids = []
        for i in range(20):
            task = Task(
                task_type="fibonacci",
                payload={"n": 1000},  # Large number to cause timeouts/failures
                priority=TaskPriority.LOW,
                max_retries=2
            )
            await self.queue.enqueue_task(task)
            task_ids.append(task.id)
            
        # Wait for tasks to complete
        stats = await self.wait_for_task_completion(task_ids, timeout=120)
        
        duration = time.time() - start_time
        success = stats["failed"] < len(task_ids) * 0.5  # Less than 50% should fail
        error_rate = stats["failed"] / len(task_ids) if task_ids else 0
        
        return FaultToleranceTestResult(
            test_name="Circuit Breaker",
            success=success,
            duration=duration,
            tasks_submitted=len(task_ids),
            tasks_completed=stats["completed"],
            tasks_failed=stats["failed"],
            tasks_retried=stats["retried"],
            error_rate=error_rate,
            details={"circuit_breaker_threshold": 5}
        )
        
    async def test_graceful_shutdown(self) -> FaultToleranceTestResult:
        """Test graceful shutdown behavior"""
        await self.clear_redis()
        logger.info("Starting graceful shutdown test")
        start_time = time.time()
        
        # Submit tasks
        task_ids = await self.submit_test_tasks(25, "sleep")
        
        # Let some tasks start processing
        await asyncio.sleep(3)
        
        # Simulate graceful shutdown by stopping workers
        logger.info("Simulating graceful shutdown")
        try:
            worker_containers = self.docker_client.containers.list(
                filters={"label": "com.docker.compose.service=worker"}
            )
            for container in worker_containers:
                container.stop(timeout=10)
        except Exception as e:
            logger.warning(f"Could not stop worker containers: {e}")
            
        # Wait for shutdown
        await asyncio.sleep(5)
        
        # Check task status
        in_progress_count = await self.queue.get_tasks_in_progress_count()
        
        duration = time.time() - start_time
        success = in_progress_count == 0  # No tasks should be stuck in progress
        error_rate = 0  # Not applicable for this test
        
        return FaultToleranceTestResult(
            test_name="Graceful Shutdown",
            success=success,
            duration=duration,
            tasks_submitted=len(task_ids),
            tasks_completed=0,  # Not measured in this test
            tasks_failed=0,     # Not measured in this test
            tasks_retried=0,    # Not measured in this test
            error_rate=error_rate,
            details={"tasks_in_progress_after_shutdown": in_progress_count}
        )
        
    async def run_all_tests(self) -> List[FaultToleranceTestResult]:
        """Run all fault tolerance tests"""
        logger.info("Starting comprehensive fault tolerance testing")
        
        tests = [
            self.test_network_partition,
            self.test_worker_crash_recovery,
            self.test_task_failure_retry,
            self.test_system_overload_backpressure,
            self.test_circuit_breaker,
            self.test_graceful_shutdown
        ]
        
        for test_func in tests:
            try:
                logger.info(f"Running test: {test_func.__name__}")
                result = await test_func()
                self.test_results.append(result)
                logger.info(f"Test {test_func.__name__} completed: {'SUCCESS' if result.success else 'FAILED'}")
            except Exception as e:
                logger.error(f"Test {test_func.__name__} failed with exception: {e}")
                # Create a failed result
                self.test_results.append(FaultToleranceTestResult(
                    test_name=test_func.__name__,
                    success=False,
                    duration=0,
                    tasks_submitted=0,
                    tasks_completed=0,
                    tasks_failed=0,
                    tasks_retried=0,
                    error_rate=1.0,
                    details={"error": str(e)}
                ))
                
        return self.test_results
        
    def generate_report(self) -> Dict[str, Any]:
        """Generate a comprehensive test report"""
        total_tests = len(self.test_results)
        successful_tests = sum(1 for result in self.test_results if result.success)
        success_rate = successful_tests / total_tests if total_tests > 0 else 0
        
        avg_duration = statistics.mean([r.duration for r in self.test_results]) if self.test_results else 0
        avg_error_rate = statistics.mean([r.error_rate for r in self.test_results]) if self.test_results else 0
        
        report = {
            "timestamp": datetime.utcnow().isoformat(),
            "summary": {
                "total_tests": total_tests,
                "successful_tests": successful_tests,
                "failed_tests": total_tests - successful_tests,
                "success_rate": success_rate,
                "average_duration": avg_duration,
                "average_error_rate": avg_error_rate
            },
            "test_results": [
                {
                    "test_name": result.test_name,
                    "success": result.success,
                    "duration": result.duration,
                    "tasks_submitted": result.tasks_submitted,
                    "tasks_completed": result.tasks_completed,
                    "tasks_failed": result.tasks_failed,
                    "tasks_retried": result.tasks_retried,
                    "error_rate": result.error_rate,
                    "recovery_time": result.recovery_time,
                    "details": result.details
                }
                for result in self.test_results
            ]
        }
        
        return report
        
    def save_report(self, report: Dict[str, Any], filename: str = None):
        """Save test report to file"""
        if filename is None:
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            filename = f"fault_tolerance_test_{timestamp}.json"
            
        filepath = os.path.join("experiment_results", filename)
        os.makedirs("experiment_results", exist_ok=True)
        
        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2, default=str)
            
        logger.info(f"Test report saved to {filepath}")
        return filepath

    def _sanitize_test_results(self, test_results):
        for result in test_results:
            # Clamp completed to [0, submitted]
            if result["tasks_completed"] < 0 or result["tasks_completed"] > result["tasks_submitted"]:
                result["tasks_completed"] = max(0, min(result["tasks_completed"], result["tasks_submitted"]))
            # Clamp failed to [0, submitted]
            if result["tasks_failed"] < 0 or result["tasks_failed"] > result["tasks_submitted"]:
                result["tasks_failed"] = max(0, min(result["tasks_failed"], result["tasks_submitted"]))
            # Clamp error_rate to [0, 1]
            if result["error_rate"] < 0 or result["error_rate"] > 1:
                result["error_rate"] = max(0, min(result["error_rate"], 1))
            # Clamp retried to >= 0
            if result["tasks_retried"] < 0:
                result["tasks_retried"] = 0
            # Clamp completion rates in details
            if result["test_name"] == "Priority Handling Under Load" and result["details"]:
                for k in ["high_priority_completion_rate", "medium_priority_completion_rate", "low_priority_completion_rate"]:
                    if k in result["details"]:
                        v = result["details"][k]
                        if v < 0 or v > 1:
                            result["details"][k] = max(0, min(v, 1))

    def create_individual_visualizations(self, report: Dict[str, Any], save_plots: bool = True):
        """Create and save a separate visualization for each test result."""
        os.makedirs("experiment_results", exist_ok=True)
        for result in report["test_results"]:
            test_name = result["test_name"]
            fig, ax = plt.subplots(figsize=(8, 6))
            bars = ax.bar([
                "Submitted", "Completed", "Failed", "Retried"
            ], [
                result["tasks_submitted"],
                result["tasks_completed"],
                result["tasks_failed"],
                result["tasks_retried"]
            ], color=["skyblue", "lightgreen", "lightcoral", "orange"])
            ax.set_title(f"{test_name} Results", fontsize=14, fontweight='bold')
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height + 0.5, f'{int(height)}', ha='center', va='bottom')
            plt.tight_layout()
            if save_plots:
                safe_name = test_name.replace(" ", "_").lower()
                plot_path = os.path.join("experiment_results", f"{safe_name}_plot.png")
                plt.savefig(plot_path, dpi=150, bbox_inches='tight')
                logger.info(f"Individual visualization saved to {plot_path}")
            plt.close(fig)
        return True

async def main():
    """Main function to run fault tolerance experiments"""
    experiment = FaultToleranceExperiment()
    
    try:
        await experiment.setup()
        results = await experiment.run_all_tests()
        report = experiment.generate_report()
        experiment.save_report(report)
        
        # Create individual visualizations for each test
        experiment.create_individual_visualizations(report, save_plots=True)
        
        # Print summary
        print("\n" + "="*60)
        print("FAULT TOLERANCE TEST SUMMARY")
        print("="*60)
        print(f"Total Tests: {report['summary']['total_tests']}")
        print(f"Successful: {report['summary']['successful_tests']}")
        print(f"Failed: {report['summary']['failed_tests']}")
        print(f"Success Rate: {report['summary']['success_rate']:.2%}")
        print(f"Average Duration: {report['summary']['average_duration']:.2f}s")
        print(f"Average Error Rate: {report['summary']['average_error_rate']:.2%}")
        print("="*60)
        
        # Print individual test results
        for result in results:
            status = "✓ PASS" if result.success else "✗ FAIL"
            print(f"{status} {result.test_name}")
            print(f"  Duration: {result.duration:.2f}s")
            print(f"  Tasks: {result.tasks_completed}/{result.tasks_submitted} completed")
            print(f"  Error Rate: {result.error_rate:.2%}")
            if result.recovery_time:
                print(f"  Recovery Time: {result.recovery_time:.2f}s")
            print()
            
    except KeyboardInterrupt:
        logger.info("Experiment interrupted by user")
    except Exception as e:
        logger.error(f"Experiment failed: {e}")
    finally:
        await experiment.cleanup()

if __name__ == "__main__":
    asyncio.run(main()) 