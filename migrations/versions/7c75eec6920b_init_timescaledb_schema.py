"""init_timescaledb_schema

Revision ID: 7c75eec6920b
Revises:
Create Date: 2026-06-14 12:05:03.600641

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "7c75eec6920b"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Enable required extensions
    op.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')

    # 2. Provisioning Table (Tenants)
    op.execute("""
        CREATE TABLE tenants (
            tenant_id VARCHAR(50) PRIMARY KEY,
            company_name VARCHAR(255) NOT NULL,
            api_key_hash VARCHAR(64) NOT NULL UNIQUE,
            is_active BOOLEAN DEFAULT TRUE NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
        );
    """)

    # 3. Time-Series Base Table
    op.execute("""
        CREATE TABLE telemetry_logs (
            timestamp TIMESTAMPTZ NOT NULL,
            log_id UUID DEFAULT uuid_generate_v4() NOT NULL,
            tenant_id VARCHAR(50) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
            service_name VARCHAR(100) NOT NULL,
            environment VARCHAR(20) NOT NULL,
            log_level VARCHAR(10) NOT NULL,
            trace_id VARCHAR(32),
            span_id VARCHAR(16),
            message TEXT NOT NULL,
            duration_ms DOUBLE PRECISION,
            http_status INT,
            metadata JSONB,
            PRIMARY KEY (timestamp, log_id)
        );
    """)

    # 4. TimescaleDB Conversion
    # This must occur after table creation to partition the data chunks by time
    op.execute(
        "SELECT create_hypertable('telemetry_logs', 'timestamp', chunk_time_interval => INTERVAL '7 days');"
    )

    # 5. High-Performance Indexing
    op.execute(
        "CREATE INDEX idx_telemetry_tenant_time ON telemetry_logs (tenant_id, timestamp DESC);"
    )
    op.execute(
        "CREATE INDEX idx_telemetry_trace_id ON telemetry_logs (trace_id) WHERE trace_id IS NOT NULL;"
    )
    op.execute(
        "CREATE INDEX idx_telemetry_errors ON telemetry_logs (tenant_id, timestamp DESC) WHERE log_level IN ('ERROR', 'CRITICAL');"
    )
    op.execute(
        "CREATE INDEX idx_telemetry_metadata_gin ON telemetry_logs USING gin (metadata);"
    )


def downgrade() -> None:
    # Safely teardown in reverse dependency order
    op.execute("DROP TABLE IF EXISTS telemetry_logs;")
    op.execute("DROP TABLE IF EXISTS tenants;")
    op.execute('DROP EXTENSION IF EXISTS "uuid-ossp";')
