import asyncio
import aiohttp
import time
import json
import csv
from datetime import datetime
import statistics
import os
from typing import List, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExperimentRunner:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.results_dir = "experiment_results"
        os.makedirs(self.results_dir, exist_ok=True)

    async def run_load_test(
        self,
        concurrent_users: int,
        duration_seconds: int,
        request_pattern: str
    ) -> Dict:
        """Run a load test with specified parameters"""
        start_time = time.time()
        tasks = []
        results = []

        async with aiohttp.ClientSession() as session:
            for _ in range(concurrent_users):
                task = asyncio.create_task(
                    self._user_session(session, duration_seconds, request_pattern, results)
                )
                tasks.append(task)
            
            await asyncio.gather(*tasks)

        end_time = time.time()
        
        return self._analyze_results(results, end_time - start_time, concurrent_users)

    async def _user_session(
        self,
        session: aiohttp.ClientSession,
        duration_seconds: int,
        pattern: str,
        results: List
    ):
        """Simulate a user session"""
        end_time = time.time() + duration_seconds

        while time.time() < end_time:
            start_request = time.time()
            try:
                if pattern == "fibonacci":
                    response = await self._submit_fibonacci_task(session)
                elif pattern == "matrix":
                    response = await self._submit_matrix_task(session)
                else:
                    raise ValueError(f"Unknown pattern: {pattern}")

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

            await asyncio.sleep(1)  # Rate limiting

    async def _submit_fibonacci_task(self, session: aiohttp.ClientSession) -> aiohttp.ClientResponse:
        """Submit a Fibonacci calculation task"""
        payload = {
            "task_type": "fibonacci",
            "payload": {"n": 30},
            "priority": 1
        }
        async with session.post(f"{self.base_url}/tasks/", json=payload) as response:
            await response.json()
            return response

    async def _submit_matrix_task(self, session: aiohttp.ClientSession) -> aiohttp.ClientResponse:
        """Submit a matrix multiplication task"""
        payload = {
            "task_type": "matrix_multiplication",
            "payload": {"size": 100},
            "priority": 1
        }
        async with session.post(f"{self.base_url}/tasks/", json=payload) as response:
            await response.json()
            return response

    def _analyze_results(
        self,
        results: List[Dict],
        duration: float,
        concurrent_users: int
    ) -> Dict:
        """Analyze test results"""
        latencies = [r["latency"] for r in results]
        successful_requests = len([r for r in results if 200 <= r["status"] < 300])
        failed_requests = len(results) - successful_requests

        return {
            "total_requests": len(results),
            "successful_requests": successful_requests,
            "failed_requests": failed_requests,
            "avg_latency": statistics.mean(latencies),
            "p95_latency": statistics.quantiles(latencies, n=20)[18],  # 95th percentile
            "p99_latency": statistics.quantiles(latencies, n=100)[98],  # 99th percentile
            "min_latency": min(latencies),
            "max_latency": max(latencies),
            "requests_per_second": len(results) / duration,
            "concurrent_users": concurrent_users,
            "duration_seconds": duration
        }

    def save_results(self, results: Dict, experiment_name: str):
        """Save experiment results to CSV and JSON"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = f"{self.results_dir}/{experiment_name}_{timestamp}"

        # Save detailed JSON
        with open(f"{base_filename}.json", "w") as f:
            json.dump(results, f, indent=2)

        # Save CSV summary
        with open(f"{base_filename}.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=results.keys())
            writer.writeheader()
            writer.writerow(results)

async def main():
    # Example experiment configurations
    experiments = [
        {"name": "baseline", "users": 10, "duration": 300, "pattern": "fibonacci"},
        {"name": "high_load", "users": 50, "duration": 300, "pattern": "fibonacci"},
        {"name": "stress_test", "users": 100, "duration": 300, "pattern": "matrix"}
    ]

    runner = ExperimentRunner()

    for exp in experiments:
        logger.info(f"Running experiment: {exp['name']}")
        results = await runner.run_load_test(
            concurrent_users=exp["users"],
            duration_seconds=exp["duration"],
            request_pattern=exp["pattern"]
        )
        runner.save_results(results, exp["name"])
        logger.info(f"Completed experiment: {exp['name']}")
        logger.info(f"Results: {json.dumps(results, indent=2)}")

if __name__ == "__main__":
    asyncio.run(main()) 