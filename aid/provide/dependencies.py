import os
from typing import Annotated

from fastapi import Security, HTTPException
from fastapi.security import APIKeyHeader
from starlette.status import HTTP_403_FORBIDDEN

api_key_header = APIKeyHeader(name="x-api-key", auto_error=False)


def get_api_key(resolved_header: Annotated[str, Security(api_key_header)]) -> str:
    """
    Validate the API key passed in the HTTP request header.
    If there is ever a need to pass the key as query parameter too, see the example here:
    https://joshdimella.com/blog/adding-api-key-auth-to-fast-api
    """
    if resolved_header == os.getenv("API_KEY"):
        return resolved_header
    else:
        raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="API key missing or invalid")
