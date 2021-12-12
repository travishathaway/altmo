import sys

import click

from altmo.settings import PG_DSN, CONFIG_DATA
from altmo.utils import get_amenity_pivot_columns, get_amenity_category_weights
from altmo.data.decorators import psycopg2_cur
from altmo.data.read import get_study_area, get_residence_composite_average_times
from altmo.data.write import (
    add_residence_amenity_category_distances,
    add_category_time_zscores,
    add_category_time_zscores_all
)

AMENITY_LEVEL_SINGLE = 'single'
AMENITY_LEVEL_CATEGORY = 'category'


@click.command('zscores')
@click.argument('study_area')
@click.option('-m', '--mode', type=str, default='pedestrian')
@click.option('-l', '--level', type=str, default=AMENITY_LEVEL_SINGLE)
@psycopg2_cur(PG_DSN)
def calculate_zscores(cursor, study_area, mode, level):
    """
    Calculates the zscore for distance and time on the `residence_amenity_distances`
    and stores it in a table called `residence_amenity_distance_standardized`
    study_area_id, *_ = get_study_area(cursor, study_area)
    if not study_area_id:
        click.echo("study area not found")
        sys.exit(1)

    if level == AMENITY_LEVEL_SINGLE:
        add_residence_amenity_category_distances(cursor, study_area_id, mode)
    elif level == AMENITY_LEVEL_CATEGORY:
        add_category_time_zscores(cursor, study_area_id, mode)
        add_category_time_zscores_all(cursor, study_area_id, mode)
    else:
        click.echo(
            f"Invalid option specified for level. "
            f"Please choose either '{AMENITY_LEVEL_CATEGORY}' or '{AMENITY_LEVEL_SINGLE}'"
        )
    """
    study_area_id, *_ = get_study_area(cursor, study_area)
    pivot_columns = get_amenity_pivot_columns(CONFIG_DATA)
    weights = get_amenity_category_weights(CONFIG_DATA)

    averages = get_residence_composite_average_times(
        cursor, study_area_id, mode, CONFIG_DATA['amenities']['categories']
    )

    from pprint import pprint
    pprint(averages)
    # get_residence_composite_average_times_2(cursor, study_area_id, mode, CONFIG_DATA['amenities']['categories'])
