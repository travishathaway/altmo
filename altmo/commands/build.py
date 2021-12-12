import click

from altmo.settings import PG_DSN, CONFIG_DATA
from altmo.data.write import (
    add_amenities, delete_amenities, add_amenities_category,
    add_residences, delete_residences,
    add_natural_amenities
)
from altmo.data.read import get_study_area
from altmo.data.schema import psycopg2_cur
from altmo.utils import get_amenities_from_config, get_amenity_category_map


@click.command()
@click.argument('study_area', type=str)
@psycopg2_cur(PG_DSN)
def build(cursor, study_area):
    """Builds a database of amenities and residences from OSM data"""
    study_area_id, *_ = get_study_area(cursor, study_area)
    amenities = get_amenities_from_config(CONFIG_DATA)
    amenity_category_map = get_amenity_category_map(CONFIG_DATA)

    if study_area_id:
        # Add amenity data
        delete_amenities(cursor, study_area_id)
        add_amenities(cursor, study_area_id, amenities)
        add_amenities_category(cursor, study_area_id, amenity_category_map)
        add_natural_amenities(cursor, study_area_id)

        # Add residence data
        delete_residences(cursor, study_area_id)
        add_residences(cursor, study_area_id)

    else:
        click.echo('study area not found')
