import click

from altmo.settings import PG_DSN
from altmo.data.write import add_amenities, delete_amenities, add_amenities_category
from altmo.data.read import get_study_area
from altmo.data.schema import psycopg2_cur


@click.command()
@click.argument('study_area', type=str)
@psycopg2_cur(PG_DSN)
def build(cursor, study_area):
    """used to build database of amenities from OSM data"""
    study_area_id, *_ = get_study_area(cursor, study_area)

    if study_area_id:
        delete_amenities(cursor, study_area_id)
        add_amenities(cursor, study_area_id)
        add_amenities_category(cursor, study_area_id)
    else:
        click.echo('study area not found')
