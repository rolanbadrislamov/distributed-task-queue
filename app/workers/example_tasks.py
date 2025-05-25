import asyncio
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

async def fibonacci_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate Fibonacci number at position n"""
    n = payload.get("n")
    if not isinstance(n, int) or n < 0:
        raise ValueError("Invalid input: n must be a non-negative integer")

    if n <= 1:
        return {"result": n}

    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    
    return {"result": b}

async def prime_factors_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate prime factors of a number"""
    n = payload.get("n")
    if not isinstance(n, int) or n < 2:
        raise ValueError("Invalid input: n must be an integer greater than 1")

    factors = []
    d = 2
    while n > 1:
        while n % d == 0:
            factors.append(d)
            n //= d
        d += 1
        if d * d > n:
            if n > 1:
                factors.append(n)
            break

    return {"result": factors}

async def matrix_multiply_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Multiply two matrices"""
    matrix_a = payload.get("matrix_a")
    matrix_b = payload.get("matrix_b")

    if not all(isinstance(m, list) for m in [matrix_a, matrix_b]):
        raise ValueError("Invalid input: matrices must be 2D lists")

    if not matrix_a or not matrix_b or not matrix_a[0] or not matrix_b[0]:
        raise ValueError("Invalid input: empty matrices")

    if len(matrix_a[0]) != len(matrix_b):
        raise ValueError("Invalid dimensions for matrix multiplication")

    # Simulate computation-intensive task
    await asyncio.sleep(2)

    result = [[sum(a * b for a, b in zip(row_a, col_b))
               for col_b in zip(*matrix_b)]
              for row_a in matrix_a]

    return {"result": result}

# Register these handlers in your worker
def register_example_handlers(worker):
    """Register all example task handlers with the worker"""
    worker.register_task_handler("fibonacci", fibonacci_task)
    worker.register_task_handler("prime_factors", prime_factors_task)
    worker.register_task_handler("matrix_multiply", matrix_multiply_task) 