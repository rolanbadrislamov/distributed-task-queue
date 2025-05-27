import asyncio
import aiohttp
import time
import json
import csv
from datetime import datetime
import statistics
import os
import subprocess
from typing import List, Dict, Optional
import logging
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WorkerScalingExperiment:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.results_dir = "experiment_results"
        os.makedirs(self.results_dir, exist_ok=True)

    async def scale_workers(self, worker_count: int) -> bool:
        """Scale the number of workers using docker-compose"""
        try:
            logger.info(f"Scaling workers to {worker_count}")
            # Run docker-compose command to scale workers
            subprocess.run(
                ["docker-compose", "up", "-d",
                    "--scale", f"worker={worker_count}"],
                check=True,
                capture_output=True,
                text=True
            )
            # Wait for workers to be ready
            # Give time for workers to start and register
            await asyncio.sleep(20)
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to scale workers: {e.stderr}")
            return False

    async def run_load_test(
        self,
        concurrent_users: int,
        duration_seconds: int,
        worker_count: int
    ) -> Dict:
        """Run a load test with specified parameters"""
        # First scale the workers
        if not await self.scale_workers(worker_count):
            raise Exception(f"Failed to scale to {worker_count} workers")

        # Get initial metrics from all workers
        initial_metrics = await self._get_all_worker_metrics()
        initial_completed_tasks = sum(
            self._extract_metric_value(metrics, 'tasks_processed_total') or 0 
            for metrics in initial_metrics.values()
        )

        start_time = time.time()
        tasks = []
        submitted_tasks = []

        async with aiohttp.ClientSession() as session:
            for _ in range(concurrent_users):
                task = asyncio.create_task(
                    self._user_session(session, duration_seconds, submitted_tasks)
                )
                tasks.append(task)

            await asyncio.gather(*tasks)

        # Get final metrics from all workers
        final_metrics = await self._get_all_worker_metrics()
        final_completed_tasks = sum(
            self._extract_metric_value(metrics, 'tasks_processed_total') or 0 
            for metrics in final_metrics.values()
        )

        end_time = time.time()
        actual_completed_tasks = final_completed_tasks - initial_completed_tasks

        return self._analyze_results(
            submitted_tasks, 
            end_time - start_time, 
            concurrent_users, 
            worker_count, 
            actual_completed_tasks,
            initial_metrics,
            final_metrics
        )

    async def _get_all_worker_metrics(self) -> Dict[str, str]:
        """Get metrics from all worker instances"""
        metrics = {}
        async with aiohttp.ClientSession() as session:
            # Get API metrics
            try:
                async with session.get(f"{self.base_url}/metrics") as response:
                    if response.status == 200:
                        metrics['api'] = await response.text()
                    else:
                        logger.error(f"Failed to get API metrics. Status: {response.status}")
            except Exception as e:
                logger.error(f"Failed to get API metrics: {e}")

            # Get a list of worker ports using docker-compose ps
            try:
                result = subprocess.run(
                    ["docker", "ps", "--format", "{{.Names}}\t{{.Ports}}"],
                    capture_output=True,
                    text=True,
                    check=True
                )
                
                # Parse the output to get worker container information
                for line in result.stdout.splitlines():
                    if not line.strip():
                        continue
                    try:
                        name, ports = line.split('\t')
                        if 'worker' in name:
                            # Extract the host port from the port mapping (e.g., 0.0.0.0:50891->8000/tcp)
                            port_match = ports.split(',')[0]  # Get first port mapping if multiple
                            host_port = port_match.split(':')[1].split('->')[0]
                            
                            # Get metrics from the worker
                            try:
                                worker_url = f"http://localhost:{host_port}/metrics"
                                async with session.get(worker_url) as response:
                                    if response.status == 200:
                                        metrics[name] = await response.text()
                                    else:
                                        logger.error(f"Failed to get metrics from {name}. Status: {response.status}")
                            except Exception as e:
                                logger.error(f"Failed to get metrics from {name}: {e}")
                    except Exception as e:
                        logger.error(f"Failed to parse container info: {e}")
                        
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to get container information: {e.stderr}")
            except Exception as e:
                logger.error(f"Unexpected error getting worker metrics: {e}")

        return metrics

    async def _user_session(
        self,
        session: aiohttp.ClientSession,
        duration_seconds: int,
        submitted_tasks: List
    ):
        """Simulate a user session with continuous task submission"""
        end_time = time.time() + duration_seconds

        while time.time() < end_time:
            start_request = time.time()
            try:
                response = await self._submit_matrix_task(session)
                response_data = await response.json()
                submitted_tasks.append({
                    "task_id": response_data.get("id"),  # Changed from task_id to id to match API response
                    "submit_time": start_request,
                    "status": response.status
                })

            except Exception as e:
                logger.error(f"Failed to submit task: {e}")
                submitted_tasks.append({
                    "task_id": None,
                    "submit_time": start_request,
                    "status": 500,
                    "error": str(e)
                })

    async def _submit_matrix_task(self, session: aiohttp.ClientSession) -> aiohttp.ClientResponse:
        """Submit a matrix multiplication task"""
        payload = {
            "task_type": "matrix_multiply",
            "payload": {"size": 750},
            "priority": 1,
            "max_retries": 3
        }
        try:
            logger.debug(f"Submitting task to {self.base_url}/tasks/")
            async with session.post(f"{self.base_url}/tasks/", json=payload) as response:
                response_text = await response.text()
                if response.status != 200:
                    logger.error(f"Task submission failed with status {response.status}")
                    logger.error(f"Response body: {response_text}")
                    raise Exception(f"Task submission failed: {response_text}")
                logger.debug(f"Task submitted successfully: {response_text}")
                return response
        except aiohttp.ClientError as e:
            logger.error(f"Network error submitting task: {e}")
            raise
        except Exception as e:
            logger.error(f"Error submitting task: {e}")
            raise

    def _extract_metric_value(self, metrics_text: str, metric_name: str) -> Optional[int]:
        """Extract value of a specific metric from Prometheus metrics text"""
        for line in metrics_text.split('\n'):
            if line.startswith(metric_name + ' '):
                try:
                    return int(float(line.split(' ')[1]))
                except (IndexError, ValueError):
                    return None
        return None

    def _analyze_results(
        self,
        submitted_tasks: List[Dict],
        duration: float,
        concurrent_users: int,
        worker_count: int,
        actual_completed_tasks: int,
        initial_metrics: Dict[str, str],
        final_metrics: Dict[str, str]
    ) -> Dict:
        """Analyze test results"""
        # Calculate tasks completed per worker
        tasks_per_worker = {}
        for worker_id, final_metrics_text in final_metrics.items():
            if worker_id.startswith('worker_'):
                final_completed = self._extract_metric_value(final_metrics_text, 'tasks_processed_total') or 0
                initial_completed = self._extract_metric_value(initial_metrics.get(worker_id, ''), 'tasks_processed_total') or 0
                tasks_per_worker[worker_id] = final_completed - initial_completed

        successful_submissions = len([t for t in submitted_tasks if 200 <= t["status"] < 300])
        failed_submissions = len(submitted_tasks) - successful_submissions

        # Log detailed metrics
        logger.info(f"Tasks completed per worker: {json.dumps(tasks_per_worker, indent=2)}")
        logger.info(f"Total tasks submitted: {len(submitted_tasks)}")
        logger.info(f"Successful submissions: {successful_submissions}")
        logger.info(f"Failed submissions: {failed_submissions}")
        logger.info(f"Actual completed tasks: {actual_completed_tasks}")
        logger.info(f"Task completion rate: {actual_completed_tasks / len(submitted_tasks) if submitted_tasks else 0:.2%}")

        return {
            "worker_count": worker_count,
            "total_tasks_submitted": len(submitted_tasks),
            "successful_submissions": successful_submissions,
            "failed_submissions": failed_submissions,
            "actual_completed_tasks": actual_completed_tasks,
            "tasks_completed_per_second": actual_completed_tasks / duration,
            "tasks_submitted_per_second": len(submitted_tasks) / duration,
            "concurrent_users": concurrent_users,
            "duration_seconds": duration,
            "tasks_completed_per_worker": tasks_per_worker,
            "task_completion_rate": actual_completed_tasks / len(submitted_tasks) if submitted_tasks else 0
        }

    def plot_results(self, all_results: List[Dict], output_file: str):
        """Generate performance comparison plots"""
        worker_counts = [r["worker_count"] for r in all_results]
        completion_rates = [r["tasks_completed_per_second"] for r in all_results]
        completion_percentages = [r["task_completion_rate"] * 100 for r in all_results]

        # Create a figure with two subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 10))

        # Plot task completion rate
        ax1.plot(worker_counts, completion_rates, 'bo-')
        ax1.set_xlabel('Number of Workers')
        ax1.set_ylabel('Tasks Completed per Second')
        ax1.set_title('Task Processing Rate vs Number of Workers')
        ax1.grid(True)

        # Plot completion percentage
        ax2.plot(worker_counts, completion_percentages, 'ro-')
        ax2.set_xlabel('Number of Workers')
        ax2.set_ylabel('Task Completion Percentage')
        ax2.set_title('Task Completion Percentage vs Number of Workers')
        ax2.grid(True)

        plt.tight_layout()
        plt.savefig(output_file)
        plt.close()

    def save_results(self, results: List[Dict], experiment_name: str):
        """Save experiment results to CSV and JSON"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = f"{self.results_dir}/{experiment_name}_{timestamp}"

        # Save detailed JSON
        with open(f"{base_filename}.json", "w") as f:
            json.dump(results, f, indent=2)

        # Save CSV summary
        if results:
            with open(f"{base_filename}.csv", "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=results[0].keys())
                writer.writeheader()
                writer.writerows(results)

        # Generate plots
        self.plot_results(results, f"{base_filename}_plot.png")


async def main():
    # Worker scaling experiment configuration
    experiment_config = {
        "name": "worker_scaling",
        "users": 50,  # Moderate number of concurrent users
        "duration": 20,  # 1 minute per test
        "worker_counts": [3, 4, 8]  # Test with different numbers of workers
    }

    runner = WorkerScalingExperiment()
    all_results = []

    try:
        for worker_count in experiment_config["worker_counts"]:
            logger.info(f"Running test with {worker_count} workers")
            results = await runner.run_load_test(
                concurrent_users=experiment_config["users"],
                duration_seconds=experiment_config["duration"],
                worker_count=worker_count
            )
            all_results.append(results)
            logger.info(f"Completed test with {worker_count} workers")
            logger.info(f"Results: {json.dumps(results, indent=2)}")

            # Add a delay between tests to let the system stabilize
            await asyncio.sleep(30)

        # Save all results
        runner.save_results(all_results, experiment_config["name"])
        logger.info("All experiments completed")

    finally:
        # Scale back to 1 worker when done
        await runner.scale_workers(1)

if __name__ == "__main__":
    asyncio.run(main())
