from fastapi import FastAPI

app = FastAPI(title="Telemetry Engine API")


@app.get("/healthz")
async def healt_ckeck():
    """Validates the API is running iside the container."""
    return {"status": "ok", "service": "telemetry-engine"}
