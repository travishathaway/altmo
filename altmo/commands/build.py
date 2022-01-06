import click

from altmo.settings import get_config
from altmo.data.write import (
    add_amenities,
    delete_amenities,
    add_amenities_category,
    add_residences,
    delete_residences,
    add_natural_amenities,
)
from altmo.data.read import get_study_area
from altmo.data.schema import psycopg2_cur
from altmo.utils import get_amenities_from_config, get_amenity_category_map


@click.command()
@click.argument("study_area", type=str)
@psycopg2_cur()
@get_config
def build(config, cursor, study_area):
    """Builds a database of amenities and residences from OSM data"""
    study_area_id, *_ = get_study_area(cursor, study_area)
    amenities = get_amenities_from_config(config.AMENITIES)
    amenity_category_map = get_amenity_category_map(config.AMENITIES)
    nature_amenities = tuple(config.AMENITIES.get('categories', {}).get('nature', {}).keys())

    if study_area_id:
        # Add amenity data
        delete_amenities(cursor, study_area_id)
        add_amenities(cursor, study_area_id, amenities)
        add_amenities_category(cursor, study_area_id, amenity_category_map)
        if nature_amenities:
            add_natural_amenities(cursor, study_area_id, nature_amenities)

        # Add residence data
        delete_residences(cursor, study_area_id)
        add_residences(cursor, study_area_id)

    else:
        click.echo("study area not found")
