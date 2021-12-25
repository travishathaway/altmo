import asyncio

import click
from psycopg2.extensions import cursor as psycopg2_cursor

from altmo.api.valhalla import  async_http_client, ValhallaAsyncClient
from altmo.batches import (
    ValhallaReaderBatch,
    DBWriterBatch,
    batch_manager,
    BatchConfig
)
from altmo.data.decorators import psycopg2_cur
from altmo.data.result_sets import StraightDistanceResultSetContainer
from altmo.settings import MODE_PEDESTRIAN
from altmo.validators import (
    validate_study_area, validate_mode
)


@async_http_client
async def main(
    client: ValhallaAsyncClient,
    result_set: StraightDistanceResultSetContainer,
    config: BatchConfig
) -> None:
    """
    Runs the main async function for saving valhalla data to database.
    """
    for idx, data in enumerate(result_set.result_sets):
        reader_batch = ValhallaReaderBatch(data, client, config)
        write_batch = DBWriterBatch(config)

        run = batch_manager(reader_batch, write_batch)
        await run()


@click.command("network")
@click.argument("study_area", type=click.UNPROCESSED, callback=validate_study_area)
@click.option("-m", "--mode", type=click.UNPROCESSED, default=MODE_PEDESTRIAN, callback=validate_mode)
@click.option("-c", "--category", type=str)
@click.option("-n", "--name", type=str)
@psycopg2_cur()
def network_distances(cur: psycopg2_cursor, study_area, mode, category, name):
    """
    Calculate network distances between residences and amenities.

    Makes potentially hundreds of thousands of API calls so it may take some time to run.
    """
    result_set = StraightDistanceResultSetContainer(
        cur, study_area_id=study_area, batch_size=500_000,
        query_kwargs={'category': category, 'name': name}
    )
    config = BatchConfig(costing=mode)
    asyncio.run(main(result_set, config))
