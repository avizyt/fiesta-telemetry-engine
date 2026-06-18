from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone


class TelemetryLog(BaseModel):
    service_name: str = Field(..., max_length=100, description="Originating service")
    environment: str = Field(
        ..., max_length=20, pattern="^(production|staging|development)$"
    )
    log_level: str = Field(..., pattern="^(INFO|WARN|ERROR|CRITICAL)$")
    message: str = Field(..., min_length=1)

    # Optional distributed tracing fields
    trace_id: Optional[str] = Field(
        None, max_length=32, description="128-bit hex string"
    )
    span_id: Optional[str] = Field(None, max_length=16, description="64-bit hex string")

    # Optional metrics
    duration_ms: Optional[float] = Field(None, ge=0.0)
    http_status: Optional[int] = Field(None, ge=100, le=599)
    metadata: Optional[Dict[str, Any]] = Field(None, description="Custom JSON payload")

    # Auto-generate UTC timestamp if the client omits it
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class TelemetryBatch(BaseModel):
    """
    Groups multiple logs together to minimize HTTP overhead.
    Max items is clamped to prevent memory exhaustion attacks.
    """

    logs: List[TelemetryLog] = Field(..., min_items=1, max_items=500)


class LatencyMetrics(BaseModel):
    time_window: datetime
    total_requests: int
    p95_latency: Optional[float] = Field(
        None, description="95th percentile latency in ms"
    )
    p99_latency: Optional[float] = Field(
        None, description="99th percentile latency in ms"
    )
