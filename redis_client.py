import os
from redis.asyncio import Redis, ConnectionPool

# Pull from environment or fallback to Docker compose local port
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Create a global, thread-safe connection pool
# decode_responses=True ensures we get Python strings back instead of raw bytes
redis_pool = ConnectionPool.from_url(redis_url, decode_responses=True)


async def get_redis() -> Redis:
    """
    FastAPI Dependency to acquire a Redis connection from the pool.
    """
    client = Redis(connection_pool=redis_pool)
    try:
        yield client
    finally:
        # Returns the connection to the pool; does not tear down the TCP socket
        await client.aclose()
