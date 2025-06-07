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
import matplotlib.pyplot as plt
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PRIORITY_LABELS = {0: 'LOW', 1: 'MEDIUM', 2: 'HIGH'}

class TaskPriorityExperiment:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.results_dir = "experiment_results"
        os.makedirs(self.results_dir, exist_ok=True)

    async def submit_task(self, session, task_type, payload, priority):
        data = {
            "task_type": task_type,
            "payload": payload,
            "priority": priority
        }
        async with session.post(f"{self.base_url}/tasks/", json=data) as resp:
            resp_json = await resp.json()
            return resp_json.get("id"), time.time()

    async def get_task_status(self, session, task_id):
        async with session.get(f"{self.base_url}/tasks/{task_id}") as resp:
            return await resp.json()

    async def run(self, num_tasks_per_priority=10, task_type="fibonacci", payload={"n": 10}):
        tasks_info = []  # List of dicts: {task_id, priority, submit_time, complete_time, completion_order}
        all_priorities = []
        for priority in [2, 1, 0]:  # HIGH, MEDIUM, LOW
            all_priorities.extend([priority] * num_tasks_per_priority)
        random.shuffle(all_priorities)
        async with aiohttp.ClientSession() as session:
            for priority in all_priorities:
                task_id, submit_time = await self.submit_task(session, task_type, payload, priority)
                tasks_info.append({
                    "task_id": task_id,
                    "priority": priority,
                    "submit_time": submit_time,
                    "complete_time": None,
                    "completion_order": None
                })
            logger.info(f"Submitted {len(tasks_info)} tasks with varying priorities (randomized order).")

            # Poll for completion
            pending = {t["task_id"]: t for t in tasks_info}
            completion_counter = 0
            while pending:
                for task_id in list(pending.keys()):
                    status = await self.get_task_status(session, task_id)
                    if status.get("status") == "completed":
                        pending[task_id]["complete_time"] = time.time()
                        pending[task_id]["completion_order"] = completion_counter
                        logger.info(f"Task {task_id} (priority {pending[task_id]['priority']}) completed at order {completion_counter}.")
                        completion_counter += 1
                        del pending[task_id]
                await asyncio.sleep(1)
            logger.info("All tasks completed.")

        # Analyze results
        for t in tasks_info:
            t["latency"] = t["complete_time"] - t["submit_time"]
        self.save_results(tasks_info)
        self.print_summary(tasks_info)

    def plot_results(self, tasks_info: List[Dict], base_filename: str):
        # Group latencies by priority
        data = {2: [], 1: [], 0: []}
        for t in tasks_info:
            data[t["priority"]].append(t["latency"])
        plt.figure(figsize=(8, 6))
        plt.boxplot([data[2], data[1], data[0]], labels=["HIGH", "MEDIUM", "LOW"])
        plt.ylabel("Completion Latency (s)")
        plt.title("Task Completion Latency by Priority")
        plt.grid(True, axis='y', linestyle='--', alpha=0.7)
        plt.savefig(f"{base_filename}_plot.png")
        plt.close()

    def save_results(self, tasks_info: List[Dict]):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = f"{self.results_dir}/task_priority_{timestamp}"
        # Save detailed JSON
        with open(f"{base_filename}.json", "w") as f:
            json.dump(tasks_info, f, indent=2)
        # Save CSV
        with open(f"{base_filename}.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=tasks_info[0].keys())
            writer.writeheader()
            writer.writerows(tasks_info)
        # Plot and save image
        self.plot_results(tasks_info, base_filename)

    def print_summary(self, tasks_info: List[Dict]):
        print("\nTask Priority Experiment Results:")
        for priority in [2, 1, 0]:
            group = [t for t in tasks_info if t["priority"] == priority]
            latencies = [t["latency"] for t in group]
            if latencies:
                print(f"Priority {PRIORITY_LABELS[priority]}: count={len(latencies)}, avg={statistics.mean(latencies):.2f}s, min={min(latencies):.2f}s, max={max(latencies):.2f}s")
            else:
                print(f"Priority {PRIORITY_LABELS[priority]}: No tasks.")

async def main():
    experiment = TaskPriorityExperiment()
    await experiment.run(num_tasks_per_priority=100, task_type="matrix_multiply", payload={"size": 200})

if __name__ == "__main__":
    asyncio.run(main()) 