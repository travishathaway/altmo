import sys

import click

from altmo.data.read import get_study_area
from altmo.data.write import add_amenity_residence_distances_straight
from altmo.data.decorators import psycopg2_cur
from altmo.settings import PG_DSN


@click.command('straight')
@click.argument('study_area')
@click.option('-c', '--category', type=str)
@click.option('-n', '--name', type=str)
@click.option('-s', '--show-status', is_flag=True)
@psycopg2_cur(PG_DSN)
def straight_distance(cursor, study_area, category, name, show_status):
    """
    Calculates the straight line distance from a residence to the nearest amenity
    """
    study_area_id, *_ = get_study_area(cursor, study_area)
    if not study_area_id:
        click.echo("study area not found")
        sys.exit(1)

    # Add residence amenity distance
    add_amenity_residence_distances_straight(
        cursor, study_area_id,
        category=category, name=name, show_status=show_status
    )
