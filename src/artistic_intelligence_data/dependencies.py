import os

from dotenv import load_dotenv
from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader
from starlette.status import HTTP_403_FORBIDDEN

load_dotenv()

API_KEY_HEADER = APIKeyHeader(name="x-api-key", auto_error=False)
ENVIRONMENT = os.getenv("APP_ENVIRONMENT", "prd")  # set this env var to 'dev' to disable api key check
VALID_API_KEY = os.getenv("APP_API_KEY")


def verify_api_key(api_key: str = Security(API_KEY_HEADER)) -> str:
    """
    Validate the API key passed in the HTTP request header.
    If there is ever a need to pass the key as query parameter too, see the example here:
    https://joshdimella.com/blog/adding-api-key-auth-to-fast-api
    """
    if ENVIRONMENT == "dev":
        return api_key

    if not api_key or api_key != VALID_API_KEY:
        raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="API key missing or invalid")

    return api_key
