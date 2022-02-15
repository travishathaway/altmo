import asyncio
import sys

import click
from tqdm.asyncio import tqdm_asyncio

from altmo.data.decorators import psycopg2_cur
from altmo.data.read import get_study_area, get_amenity_name_category
from altmo.data.write import add_amenity_residence_distances_straight_async


@click.command("straight")
@click.argument("study_area")
@click.option("-c", "--category", type=str)
@click.option("-n", "--name", type=str)
@click.option("-s", "--show-status", is_flag=True)
@click.option("-p", "--parallel", type=int, default=1)
@psycopg2_cur()
def straight_distance(cursor, study_area, category, name, show_status, parallel):
    """
    Calculates the straight line distance from a residence to the nearest amenity.

    Use `--parallel|-p` to increase the number of concurrent queries being run against
    the database (default value is `1`).

    Cancelling this command (e.g. with Ctrl-C) will not cancel the current running queries.
    """
    study_area_id, *_ = get_study_area(cursor, study_area)
    if not study_area_id:
        click.echo("study area not found")
        sys.exit(1)

    # Add residence amenity distance
    amenities = get_amenity_name_category(
        cursor, study_area_id, category=category, name=name
    )

    async def main():
        # This limits the number of running queries, defaults to `1`
        sem = asyncio.Semaphore(parallel)

        async def task(amty, cat):
            async with sem:
                await add_amenity_residence_distances_straight_async(study_area_id, amty, cat)

        tasks = tuple(task(amty, cat) for amty, cat in amenities)

        if show_status:
            await tqdm_asyncio.gather(*tasks, unit="amenity")
        else:
            await asyncio.gather(*tasks)

    asyncio.run(main())
