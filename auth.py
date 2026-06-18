from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader
import hashlib

# Expect the client to send 'X-API-Key: <their_secret_key>'
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)


async def verify_tenant(api_key: str = Security(api_key_header)) -> str:
    """
    Validates the API key and returns the associated tenant_id.
    In production, this would hit a Redis cache mapping hashes to tenant IDs.
    """
    # For local development, we mock a successful hash matching 'tenant_prod_99x'
    hashed_key = hashlib.sha256(api_key.encode()).hexdigest()
    expected_hash = hashlib.sha256(b"dev_secret_key").hexdigest()

    if hashed_key == expected_hash:
        return "tenant_prod_99x"

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing API Key"
    )
