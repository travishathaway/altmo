import sys

import click

from altmo.data.read import (
    get_study_area,
    get_residence_points_time_zscore_geojson
)
from altmo.data.decorators import psycopg2_cur
from altmo.settings import PG_DSN


EXPORT_TYPES = (
    'study_area_parts',  # Aggregated by city parts and includes all categories
    'residences'
)


@click.command('export')
@click.argument('study_area', type=str)
@click.option('-m', '--mode', default='walking')
@psycopg2_cur(PG_DSN)
def export(cursor, study_area, mode):
    """Exports various formats of the analysis"""
    study_area_id, *_ = get_study_area(cursor, study_area)
    if not study_area_id:
        click.echo("study area not found")
        sys.exit(1)

    click.echo(get_residence_points_time_zscore_geojson(cursor, study_area_id, mode))
