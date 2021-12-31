import asyncio

import aiofiles
import click
from psycopg2.extensions import cursor as psycopg2_cursor

from altmo.api.valhalla import  async_http_client, ValhallaAsyncClient
from altmo.batches import (
    ValhallaReaderBatch,
    DBWriterBatch,
    StdOutWriterBatch,
    CSVWriterBatch,
    batch_manager,
    BatchConfig
)
from altmo.data.decorators import psycopg2_cur
from altmo.data.result_sets import StraightDistanceResultSetContainer
from altmo.settings import MODE_PEDESTRIAN
from altmo.validators import (
    validate_study_area, validate_mode, validate_out,
    OUT_DB, OUT_CSV, OUT_STDOUT
)


BATCH_WRITERS_CLS = {
    OUT_DB: DBWriterBatch,
    OUT_CSV: CSVWriterBatch,
    OUT_STDOUT: StdOutWriterBatch,
}


@async_http_client
async def run(
    client: ValhallaAsyncClient,
    result_set: StraightDistanceResultSetContainer,
    config: BatchConfig
):
    for data in result_set.result_sets:
        reader_batch = ValhallaReaderBatch(data, client, config)
        write_batch = BATCH_WRITERS_CLS[config.out](config)

        run_tasks = batch_manager(reader_batch, write_batch)
        await run_tasks()


@async_http_client
async def run_with_file(
    client: ValhallaAsyncClient,
    result_set: StraightDistanceResultSetContainer,
    config: BatchConfig
):
    for idx, data in enumerate(result_set.result_sets, start=1):
        file_name = f'{idx}-{config.file_name}'
        reader_batch = ValhallaReaderBatch(data, client, config)

        async with aiofiles.open(file_name, 'w') as fp:
            write_batch = BATCH_WRITERS_CLS[config.out](fp, config)

            run_tasks = batch_manager(reader_batch, write_batch)
            await run_tasks()


BATCH_WRITERS_FUNCS = {
    OUT_DB: run,
    OUT_STDOUT: run,
    OUT_CSV: run_with_file
}


@click.command("network")
@click.argument("study_area", type=click.UNPROCESSED, callback=validate_study_area)
@click.option("-m", "--mode", type=click.UNPROCESSED, default=MODE_PEDESTRIAN, callback=validate_mode)
@click.option("-c", "--category", type=str)
@click.option("-n", "--name", type=str)
@click.option("-o", "--out", type=click.UNPROCESSED, default=OUT_DB, callback=validate_out)
@click.option("-f", "--file-name", type=str, default="out.csv")
@psycopg2_cur()
def network_distances(cur: psycopg2_cursor, study_area, mode, category, name, out, file_name):
    """
    Calculate network distances between residences and amenities.

    INFO: Makes potentially hundreds of thousands of API calls so it may take some time to run.

    When called with --out=csv it will write a CSV file for every 500,000 rows it processes.
    This means csv files will be written with a number prefix like, "1-out.csv", "2-out.csv", etc.
    """
    result_set = StraightDistanceResultSetContainer(
        cur,
        study_area_id=study_area,
        batch_size=500_000,
        query_kwargs={'category': category, 'name': name}
    )

    config = BatchConfig(
        costing=mode,
        out=out,
        file_name=file_name
    )

    main_runner = BATCH_WRITERS_FUNCS[config.out]
    asyncio.run(main_runner(result_set, config))
