import asyncio
import aiohttp
import time
import json
import csv
from datetime import datetime
import statistics
import os
import subprocess
from typing import List, Dict
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
                ["docker-compose", "up", "-d", "--scale", f"worker={worker_count}"],
                check=True,
                capture_output=True,
                text=True
            )
            # Wait for workers to be ready
            await asyncio.sleep(20)  # Give time for workers to start and register
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

        start_time = time.time()
        tasks = []
        results = []

        async with aiohttp.ClientSession() as session:
            for _ in range(concurrent_users):
                task = asyncio.create_task(
                    self._user_session(session, duration_seconds, results)
                )
                tasks.append(task)
            
            await asyncio.gather(*tasks)

        end_time = time.time()
        
        return self._analyze_results(results, end_time - start_time, concurrent_users, worker_count)

    async def _user_session(
        self,
        session: aiohttp.ClientSession,
        duration_seconds: int,
        results: List
    ):
        """Simulate a user session with continuous task submission"""
        end_time = time.time() + duration_seconds

        while time.time() < end_time:
            start_request = time.time()
            try:
                response = await self._submit_matrix_task(session)
                results.append({
                    "latency": time.time() - start_request,
                    "status": response.status,
                    "timestamp": start_request
                })

            except Exception as e:
                results.append({
                    "latency": time.time() - start_request,
                    "status": 500,
                    "timestamp": start_request,
                    "error": str(e)
                })

    async def _submit_matrix_task(self, session: aiohttp.ClientSession) -> aiohttp.ClientResponse:
        """Submit a matrix multiplication task"""
        payload = {
            "task_type": "matrix_multiply",
            "payload": {"size": 250},  # Reduced matrix size from 750 to 250
            "priority": 1
        }
        async with session.post(f"{self.base_url}/tasks/", json=payload) as response:
            await response.json()
            return response

    def _analyze_results(
        self,
        results: List[Dict],
        duration: float,
        concurrent_users: int,
        worker_count: int
    ) -> Dict:
        """Analyze test results"""
        latencies = [r["latency"] for r in results]
        successful_requests = len([r for r in results if 200 <= r["status"] < 300])
        failed_requests = len(results) - successful_requests

        return {
            "worker_count": worker_count,
            "total_requests": len(results),
            "successful_requests": successful_requests,
            "failed_requests": failed_requests,
            "avg_latency": statistics.mean(latencies),
            "p95_latency": statistics.quantiles(latencies, n=20)[18],
            "p99_latency": statistics.quantiles(latencies, n=100)[98],
            "min_latency": min(latencies),
            "max_latency": max(latencies),
            "requests_per_second": len(results) / duration,
            "concurrent_users": concurrent_users,
            "duration_seconds": duration
        }

    def plot_results(self, all_results: List[Dict], output_file: str):
        """Generate performance comparison plots"""
        worker_counts = [r["worker_count"] for r in all_results]
        avg_latencies = [r["avg_latency"] for r in all_results]
        throughputs = [r["requests_per_second"] for r in all_results]

        # Create a figure with two subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 10))

        # Plot average latency
        ax1.plot(worker_counts, avg_latencies, 'bo-')
        ax1.set_xlabel('Number of Workers')
        ax1.set_ylabel('Average Latency (seconds)')
        ax1.set_title('Average Latency vs Number of Workers')
        ax1.grid(True)

        # Plot throughput
        ax2.plot(worker_counts, throughputs, 'ro-')
        ax2.set_xlabel('Number of Workers')
        ax2.set_ylabel('Requests per Second')
        ax2.set_title('Throughput vs Number of Workers')
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
        "duration": 60,  #  1 minute per test
        "worker_counts": [1, 2, 4, 6, 8]  # Test with different numbers of workers
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