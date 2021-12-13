import sys
from typing import List, Tuple
import json

import click
from osgeo import gdal

from altmo.settings import PG_DSN, SRS_ID, CONFIG_DATA
from altmo.data.decorators import psycopg2_cur
from altmo.data.read import (
    get_study_area, get_residence_composite_average_times
)
from altmo.utils import (
    get_available_amenity_categories, get_residence_composite_as_geojson,
    get_amenity_categories
)

AVAILABLE_FIELDS = (
    'all',
) + get_available_amenity_categories(CONFIG_DATA)


@click.command('raster')
@click.argument('study_area')
@click.argument('outfile')
@click.option('-m', '--mode', default='pedestrian')
@click.option('-f', '--field', default='all')
@click.option('-r', '--resolution', default=100)
@click.option('-s', '--srs-id', default=SRS_ID)
@psycopg2_cur(PG_DSN)
def raster(cursor, study_area, outfile, mode, field, resolution, srs_id) -> None:
    """Generates raster data from database"""
    study_area_id, *_ = get_study_area(cursor, study_area)
    if not study_area_id:
        click.echo("study area not found")
        sys.exit(1)

    if field not in AVAILABLE_FIELDS:
        click.echo(f'Field not found. Pick one of the following: \n{",".join(AVAILABLE_FIELDS)}')
        sys.exit(1)

    amenities = get_amenity_categories(CONFIG_DATA)
    cols, data = get_residence_composite_average_times(
        cursor, study_area_id, mode, amenities,
        srs_id=srs_id, include_geojson=True
    )

    geojson = get_residence_composite_as_geojson(cols, data)

    pts = [
        row['geometry']['coordinates']
        for row in geojson['features']
    ]

    [[lrx, lry], [ulx, uly]] = get_bounding_box(pts)
    x_size = round((ulx - lrx) / resolution)
    y_size = round((uly - lry) / resolution)

    # inverse distance to a power
    gdal.Grid(outfile, json.dumps(geojson), zfield=field,
              algorithm="invdist:power=5:radius1=200:radius2=200:nodata=-1",
              outputSRS=f'EPSG:{SRS_ID}',
              outputBounds=[ulx, uly, lrx, lry], width=x_size, height=y_size)


def get_bounding_box(pts: List[Tuple]) -> List:
    """
    Provided a geojson object return a flat list of points as list
    """
    x_vals = [x for x, _ in pts]
    y_vals = [y for _, y in pts]
    min_x = min(x_vals)
    max_x = max(x_vals)
    min_y = min(y_vals)
    max_y = max(y_vals)

    return [[min_x, min_y], [max_x, max_y]]
