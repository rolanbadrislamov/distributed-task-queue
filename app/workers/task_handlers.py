import asyncio
import concurrent.futures
import random

_executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)

async def fibonacci_handler(payload):
    n = payload.get('n', 0)
    def fib(n):
        a, b = 0, 1
        for _ in range(n):
            a, b = b, a + b
        return a
    return await asyncio.to_thread(fib, n)


async def matrix_multiply_handler(payload):
    size = payload.get('size', 10) # Default to 10x10 if not specified
    if not isinstance(size, int) or size <= 0:
        raise ValueError('Matrix size must be a positive integer')

    # Generate two random matrices of the given size
    # For simplicity, using random integers between 0 and 9
    # In a real scenario, these might come from a more complex source or be floats.
    A = [[random.randint(0, 9) for _ in range(size)] for _ in range(size)]
    B = [[random.randint(0, 9) for _ in range(size)] for _ in range(size)]

    if not A or not B: # Should not happen with current generation logic
        raise ValueError('Failed to generate matrices A or B')
        
    def matmul(A, B):
        return [[sum(x*y for x, y in zip(row, col)) for col in zip(*B)] for row in A]
    return await asyncio.to_thread(matmul, A, B)


async def sleep_handler(payload):
    duration = payload.get('duration', 1)
    await asyncio.sleep(duration)
    return f'Slept for {duration} seconds'
