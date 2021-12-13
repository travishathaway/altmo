import json

import click

from altmo.settings import PG_DSN, CONFIG_DATA
from altmo.data.decorators import psycopg2_cur
from altmo.data.read import get_study_area, get_residence_composite_average_times


@click.command('zscores')
@click.argument('study_area')
@click.option('-m', '--mode', type=str, default='pedestrian')
@psycopg2_cur(PG_DSN)
def calculate_zscores(cursor, study_area, mode):
    """
    Deprecated: I will most likely remove this command because it is not longer needed.
    I'm keeping here for now to show how the `get_residence_composite_average_times`
    function works.
    """
    study_area_id, *_ = get_study_area(cursor, study_area)

    averages = get_residence_composite_average_times(
        cursor, study_area_id, mode, CONFIG_DATA['amenities']['categories']
    )

    click.echo(json.dumps(averages))
