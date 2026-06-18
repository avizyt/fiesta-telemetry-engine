import asyncio
import os
import json
import logging
from datetime import datetime
import asyncpg
from redis.asyncio import Redis, ConnectionPool
from redis.exceptions import ResponseError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger("worker")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
# asyncpg requires the native postgresql:// prefix, so we strip out SQLAlchemy's +asyncpg if it exists
DB_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:password@localhost:5433/telemetry"
).replace("+asyncpg", "")

STREAM_KEY = "telemetry:ingestion:stream"
GROUP_NAME = "timescale_writers"
CONSUMER_NAME = os.getenv("HOSTNAME", "worker-1")

redis_pool = ConnectionPool.from_url(REDIS_URL, decode_responses=True)
redis = Redis(connection_pool=redis_pool)


async def init_consumer_group():
    try:
        await redis.xgroup_create(
            name=STREAM_KEY, groupname=GROUP_NAME, id="0", mkstream=True
        )
        logger.info(f"Consumer group '{GROUP_NAME}' initialized.")
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


async def process_stream(db_pool):
    """Main event loop to consume from Redis and bulk insert into TimescaleDB."""
    await init_consumer_group()
    logger.info(f"Starting consumer '{CONSUMER_NAME}' for group '{GROUP_NAME}'...")

    # Pre-compile the bulk insertion query
    # Notice $11::jsonb - we explicitly cast the stringified JSON to Postgres JSONB
    insert_query = """
        INSERT INTO telemetry_logs (
            timestamp, tenant_id, service_name, environment, log_level, 
            trace_id, span_id, message, duration_ms, http_status, metadata
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb
        )
    """

    while True:
        try:
            streams = await redis.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_KEY: ">"},
                count=500,
                block=2000,
            )

            if not streams:
                continue

            for stream_name, messages in streams:
                db_batch = []
                message_ids = []

                for message_id, fields in messages:
                    try:
                        tenant_id = fields["tenant_id"]
                        log_data = json.loads(fields["payload"])

                        # Extract and format the data strictly for the asyncpg query
                        db_batch.append(
                            (
                                datetime.fromisoformat(log_data["timestamp"]),
                                tenant_id,
                                log_data["service_name"],
                                log_data["environment"],
                                log_data["log_level"],
                                log_data.get("trace_id"),
                                log_data.get("span_id"),
                                log_data["message"],
                                log_data.get("duration_ms"),
                                log_data.get("http_status"),
                                json.dumps(log_data.get("metadata"))
                                if log_data.get("metadata")
                                else None,
                            )
                        )
                        message_ids.append(message_id)
                    except Exception as parse_err:
                        logger.error(f"Failed to parse log {message_id}: {parse_err}")

                if db_batch:
                    # 1. Open a transaction and execute the bulk insert
                    async with db_pool.acquire() as conn:
                        async with conn.transaction():
                            await conn.executemany(insert_query, db_batch)

                    logger.info(
                        f"Successfully wrote micro-batch of {len(db_batch)} logs to TimescaleDB."
                    )

                    # 2. Acknowledge messages in Redis ONLY after the DB transaction commits safely
                    await redis.xack(STREAM_KEY, GROUP_NAME, *message_ids)

        except asyncio.CancelledError:
            logger.info("Worker shutting down gracefully...")
            break
        except Exception as e:
            logger.error(f"Database/Worker error: {e}", exc_info=True)
            await asyncio.sleep(5)


async def main():
    # Initialize the high-performance asyncpg connection pool
    logger.info("Connecting to TimescaleDB...")
    db_pool = await asyncpg.create_pool(DB_URL, min_size=5, max_size=20)
    try:
        await process_stream(db_pool)
    finally:
        await db_pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker terminated by user.")
