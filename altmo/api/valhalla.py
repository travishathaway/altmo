from functools import wraps

import aiohttp

from altmo.settings import get_config_method, Config
from altmo.data.types import Point


def async_http_client(func):
    """Provides an async http client to functions"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        async with aiohttp.ClientSession() as session:
            client = ValhallaAsyncClient(session)
            return await func(client, *args, **kwargs)
    return wrapper


def async_http_client_method(func):
    """Provides an async http client to class methods"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        async with aiohttp.ClientSession() as session:
            client = ValhallaAsyncClient(session)
            self, *_args = args
            return await func(self, client, *_args, **kwargs)
    return wrapper


def get_matrix_request(residence: Point, amenities: list[Point], costing: str = "auto") -> dict:
    """
    This returns the JSON serializable object that we pass to the matrix API.

    More information about the API request and response here:
        - https://valhalla.readthedocs.io/en/latest/api/matrix/api-reference/
    """
    sources = [{"lat": residence.lat, "lon": residence.lng}]
    targets = [{"lat": row.lat, "lon": row.lng} for row in amenities]

    return {
        "sources": sources,
        "targets": targets,
        "costing": costing,
    }


class ValhallaAsyncClient:
    """
    Thin wrapper around aiohttp.ClientSession to provide async methods for Valhalla API
    """
    def __init__(self, session: aiohttp.ClientSession):
        self._session = session

    @get_config_method
    async def source_to_targets(self, config: Config, *args, **kwargs):
        url = f'{config.VALHALLA_SERVER}/sources_to_targets'
        return await (await self._session.post(url, *args, **kwargs)).json()
