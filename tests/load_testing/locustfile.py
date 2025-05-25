from locust import HttpUser, task, between
import random
import json

class TaskQueueUser(HttpUser):
    wait_time = between(1, 3)
    
    @task(3)
    def submit_fibonacci_task(self):
        payload = {
            "task_type": "fibonacci",
            "payload": {"n": random.randint(10, 30)},
            "priority": random.randint(1, 3),
            "max_retries": 3
        }
        self.client.post("/tasks/", json=payload)
    
    @task(1)
    def get_task_status(self):
        # Implement task status checking
        task_id = "some-task-id"  # You'll need to store and use real task IDs
        self.client.get(f"/tasks/{task_id}")
    
    @task(1)
    def get_queue_stats(self):
        self.client.get("/tasks/stats")

    @task(2)
    def submit_cpu_intensive_task(self):
        payload = {
            "task_type": "matrix_multiplication",
            "payload": {
                "size": random.randint(50, 200)
            },
            "priority": random.randint(1, 3)
        }
        self.client.post("/tasks/", json=payload) 