import os
from contextlib import asynccontextmanager
from typing import List
from fastapi import FastAPI, Depends, status, Query
from redis.asyncio import Redis
import asyncpg

from schemas import TelemetryBatch, LatencyMetrics
from auth import verify_tenant
from redis_client import get_redis

# Extract the database URL, ensuring it works with asyncpg natively
DB_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:password@localhost:5433/telemetry"
).replace("+asyncpg", "")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize the asyncpg connection pool for fast reads
    app.state.db_pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=10)
    yield
    # Teardown: Cleanly close the pool on shutdown
    await app.state.db_pool.close()


# Initialize FastAPI with the lifespan manager
app = FastAPI(title="Telemetry Engine API", lifespan=lifespan)


@app.get("/healthz")
async def health_check():
    return {"status": "ok", "service": "telemetry-engine"}


@app.post(
    "/api/v1/telemetry/submit",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest a batch of telemetry logs",
)
async def submit_telemetry(
    payload: TelemetryBatch,
    tenant_id: str = Depends(verify_tenant),
    redis: Redis = Depends(get_redis),
):
    """
    Fast-path ingestion endpoint.
    Validates schema, batches commands, and buffers them in Redis Streams.
    """
    stream_key = "telemetry:ingestion:stream"

    # Use a pipeline to minimize TCP round-trips to the Redis server
    # transaction=False means we don't need strict ACID MULTI/EXEC blocking here
    async with redis.pipeline(transaction=False) as pipe:
        for log in payload.logs:
            pipe.xadd(
                name=stream_key,
                fields={
                    "tenant_id": tenant_id,
                    # Serialize the Pydantic model to a JSON string for the worker
                    "payload": log.model_dump_json(),
                },
            )
        # Execute the entire batch in one network call
        await pipe.execute()

    return {
        "status": "accepted",
        "tenant_id": tenant_id,
        "logs_queued": len(payload.logs),
    }


@app.get(
    "/api/v1/telemetry/metrics/latency",
    response_model=List[LatencyMetrics],
    summary="get aggregated latency metrics over time",
)
async def get_latency_metrics(
    tenant_id: str = Depends(verify_tenant),
    interval: str = Query(
        "5 minutes", description="Time bucket interval (e.g., '1 minute', '1 hour')"
    ),
    hours_back: int = Query(
        2, ge=1, le=168, description="How many hours of history to query"
    ),
):
    """
    Executes a high-performance time-series query to calculate p95 and p99 latencies.
    """
    query = """
        SELECT 
            time_bucket($1::text::interval, timestamp) AS time_window,
            COUNT(*) as total_requests,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_latency,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms) AS p99_latency
        FROM telemetry_logs
        WHERE tenant_id = $2 
          AND timestamp > NOW() - ($3::int * INTERVAL '1 hour')
          AND duration_ms IS NOT NULL
        GROUP BY time_window
        ORDER BY time_window DESC;
    """

    # Acquire a connection from the global pool
    async with app.state.db_pool.acquire() as conn:
        # asyncpg returns Record objects, which behave like dictionaries
        records = await conn.fetch(query, interval, tenant_id, hours_back)

    return [dict(r) for r in records]
