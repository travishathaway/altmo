import click

from altmo.settings import PG_DSN
from altmo.data.write import (
    add_amenities, delete_amenities, add_amenities_category,
    add_residences, delete_residences, add_amenity_residence_distances_straight,
    add_natural_amenities
)
from altmo.data.read import get_study_area
from altmo.data.schema import psycopg2_cur


@click.command()
@click.argument('study_area', type=str)
@click.option('-s', '--show-status', is_flag=True)
@psycopg2_cur(PG_DSN)
def build(cursor, study_area, show_status):
    """Builds a database of amenities and residences from OSM data"""
    study_area_id, *_ = get_study_area(cursor, study_area)

    if study_area_id:
        # Add amenity data
        delete_amenities(cursor, study_area_id)
        add_amenities(cursor, study_area_id)
        add_amenities_category(cursor, study_area_id)
        add_natural_amenities(cursor, study_area_id)

        # Add residence data
        delete_residences(cursor, study_area_id)
        add_residences(cursor, study_area_id)

    else:
        click.echo('study area not found')
