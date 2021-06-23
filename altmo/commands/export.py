import sys

import click

from altmo.data.read import get_study_area, get_study_area_parts_all_geojson
from altmo.data.decorators import psycopg2_cur
from altmo.settings import PG_DSN


EXPORT_TYPES = (
    'parts_all',  # Aggregated by city parts and includes all categories
)


@click.command('export')
@click.argument('study_area', type=str)
@click.argument('export_type', type=str)
@psycopg2_cur(PG_DSN)
def export(cursor, study_area, export_type):
    """Exports various formats of the analysis"""
    study_area_id, *_ = get_study_area(cursor, study_area)
    if not study_area_id:
        click.echo("study area not found")
        sys.exit(1)

    if export_type == 'parts_all':
        click.echo(get_study_area_parts_all_geojson(cursor, study_area_id))
    else:
        click.echo('export type not recognized. available choices are')