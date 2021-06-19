import sys

import click

from altmo.settings import PG_DSN
from altmo.data.decorators import psycopg2_cur
from altmo.data.read import get_study_area
from altmo.data.write import add_standardized_network_distances


@click.command('zscores')
@click.argument('study_area')
@psycopg2_cur(PG_DSN)
def calculate_zscores(cursor, study_area):
    """
    Calculates the zscore for distance and time on the `residence_amenity_distances`
    and stores it in a new table `residence_amenity_distance_standardized`
    """
    study_area_id, *_ = get_study_area(cursor, study_area)
    if not study_area_id:
        click.echo("study area not found")
        sys.exit(1)

    add_standardized_network_distances(cursor, study_area_id)
