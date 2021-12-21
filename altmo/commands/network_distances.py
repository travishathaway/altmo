import asyncio
from collections import defaultdict
from itertools import zip_longest
import math
import sys
from typing import AsyncGenerator, Generator

import aiopg
import click
import aiohttp
import psycopg2.errors

from altmo.api.valhalla import get_matrix_request
from altmo.data.decorators import psycopg2_cur
from altmo.data.read import (
    get_residence_amenity_straight_distance_async,
    get_residence_amenity_straight_distance_count,
)
from altmo.data.write import add_amenity_residence_distance_async
from altmo.settings import get_config, MODE_PEDESTRIAN, Config
from altmo.validators import (
    validate_study_area, validate_mode, validate_parallel,
    PARALLEL_HIGH, PARALLEL_LOW
)


PARALLEL_SETTINGS = {
    PARALLEL_LOW: {
        'http': 5,
        'db': 5,
    },
    PARALLEL_HIGH: {
        'http': 40,
        'db': 20,
    },
}


@click.command("network")
@click.argument("study_area", type=click.UNPROCESSED, callback=validate_study_area)
@click.option("-m", "--mode", type=click.UNPROCESSED, default=MODE_PEDESTRIAN, callback=validate_mode)
@click.option("-c", "--category", type=str)
@click.option("-n", "--name", type=str)
@click.option("-p", "--parallel", type=click.UNPROCESSED, callback=validate_parallel, default=PARALLEL_LOW)
@psycopg2_cur
def network_distances(cursor, study_area, mode, category, name, parallel):
    """Calculate network distances between residences and amenities"""
    db_parallel_query: int = 4
    parallel_settings = PARALLEL_SETTINGS[parallel]
    total_records = get_residence_amenity_straight_distance_count(cursor, study_area)
    batch_size = math.ceil(total_records / db_parallel_query)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        main_async(
            loop, study_area, batch_size, total_records,
            mode=mode, category=category, name=name,
            parallel_http=parallel_settings['http'], parallel_db=parallel_settings['db'])
    )


@get_config
async def get_residence_batches(
        config: Config, loop, study_area_id: int, total_records: int, batch_size: int,
        category: str = None, name: str = None
) -> list[list[tuple]]:
    async with aiopg.create_pool(config.PG_DSN) as pool:
        async def _get_residence_amenity_data(_pool, _start, limit):
            async with _pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    return await get_residence_amenity_straight_distance_async(
                        cursor, study_area_id, start=_start, limit=limit, category=category, name=name
                    )
        tasks = []
        for start in range(0, total_records, batch_size):
            tasks.append(loop.create_task(_get_residence_amenity_data(pool, start, batch_size)))

        tasks, stat = await asyncio.wait(tasks)
        return [task.result() for task in tasks]


def valhalla_batches(batches: list[list[tuple]]) -> Generator:
    """
    Creates a Generator to marshall the `batches` list into a more usable format for processing.
    """
    for batch in batches:
        res_groups = defaultdict(list)
        for res_id, amt_id, res_lat, res_lng, amt_lat, amt_lng in batch:
            res_groups[(res_lat, res_lng, res_id)].append((amt_lat, amt_lng, amt_id))

        for (res_lat, res_lng, res_id), amt_data in res_groups.items():
            amt_batches = zip_longest(*(iter(amt_data),) * 50)  # Put in batches of 50
            yield {
                'res_id': res_id, 'res_lat': res_lat, 'res_lng': res_lng, 'batches': amt_batches,
            }


@get_config
async def get_valhalla_data(config: Config, residence_batches, mode: str, parallel_http: int = 20) -> AsyncGenerator:
    """
    Creates an AsyncGenerator that can be used to query and return data from the Valhalla API in parallel
    """
    async def _yield_batches(_http_batches) -> AsyncGenerator:
        answers = await asyncio.gather(*[
            asyncio.ensure_future(session.post(matrix_endpoint, json=_json_data))
            for _data, _json_data in http_batches
        ])
        for answer, (ret_data, _) in zip(answers, http_batches):
            yield (await answer.json()).get('sources_to_targets'), ret_data

    async with aiohttp.ClientSession() as session:
        http_batches = []
        matrix_endpoint = f'{config.VALHALLA_SERVER}/sources_to_targets'
        for data in valhalla_batches(residence_batches):
            for batch in data['batches']:
                res_data = (data['res_lat'], data['res_lng'])
                json_data = get_matrix_request(res_data, batch, costing=mode)
                db_data = {**data, 'batches': batch, 'mode': mode}
                http_batches.append((db_data, json_data))
            if len(http_batches) == parallel_http:
                async for batch in _yield_batches(http_batches):
                    yield batch
                http_batches = []

        # Yield any remaining items
        async for batch in _yield_batches(http_batches):
            yield batch


@get_config
async def write_valhalla_data(config, valhalla_data: AsyncGenerator, parallel_db: int = 10) -> None:
    """
    Writes data asynchronously to the database.
    We can additional control the level of parallelism via the  `parallel_queries` parameter.
    """
    async with aiopg.create_pool(config.PG_DSN) as pool:
        async def _write_residence_amenity_data(_pool, _new_records):
            async with _pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    try:
                        await add_amenity_residence_distance_async(cursor, _new_records)
                    except psycopg2.errors.UniqueViolation as exc:
                        sys.stderr.write(f'{str(exc)}\n')

        tasks = []
        async for api_data, db_data in valhalla_data:
            residence_id = db_data['res_id']
            mode = db_data['mode']

            new_records = []
            for [distance, *_], (_, _, amenity_id) in zip(api_data, db_data['batches']):
                new_records.append((
                    distance["distance"], distance["time"], amenity_id, residence_id, mode,
                ))

            tasks.append(_write_residence_amenity_data(pool, new_records))

            if len(tasks) == parallel_db:
                await asyncio.wait(tasks)
                tasks = []

        # Run any remaining tasks
        await asyncio.wait(tasks)


async def main_async(
        loop, study_area_id: int, batch_size: int, total_records: int, mode: str = 'pedestrian',
        category: str = None, name: str = None, parallel_db: int = 10, parallel_http: int = 20
) -> None:
    """
    Entry point for the main async processing for the `network` command
    """
    residence_batches = await get_residence_batches(
        loop, study_area_id, total_records, batch_size, category=category, name=name,
    )
    valhalla_data: AsyncGenerator = get_valhalla_data(residence_batches, mode, parallel_http=parallel_http)
    await write_valhalla_data(valhalla_data, parallel_db=parallel_db)
