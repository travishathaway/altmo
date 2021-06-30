import sys
from typing import List, Tuple
import json

import click
from osgeo import gdal

from altmo.settings import PG_DSN, SRS_ID
from altmo.data.decorators import psycopg2_cur
from altmo.data.read import get_study_area, get_residence_points_time_zscore_geojson

AVAILABLE_FIELDS = (
    'all_time_zscore',
    'all_average_time',
    'administrative_time_zscore',
    'administrative_average_time',
    'community_time_zscore',
    'community_average_time',
    'groceries_time_zscore',
    'groceries_average_time',
    'health_time_zscore',
    'health_average_time',
    'nature_time_zscore',
    'nature_average_time',
    'outing_destination_time_zscore',
    'outing_destination_average_time',
    'school_time_zscore',
    'school_average_time',
    'shopping_time_zscore',
    'shopping_average_time'
)


@click.command('raster')
@click.argument('study_area')
@click.argument('outfile')
@click.option('-m', '--mode', default='walking')
@click.option('-f', '--field', default='all_average_time')
@click.option('-r', '--resolution', default=100)
@psycopg2_cur(PG_DSN)
def raster(cursor, study_area, outfile, mode, field, resolution) -> None:
    """Generates raster data from database"""
    study_area_id, *_ = get_study_area(cursor, study_area)
    if not study_area_id:
        click.echo("study area not found")
        sys.exit(1)

    if field not in AVAILABLE_FIELDS:
        click.echo(f'Field not found. Pick one of the following: \n{",".join(AVAILABLE_FIELDS)}')
        sys.exit(1)

    geojson = json.loads(
        get_residence_points_time_zscore_geojson(cursor, study_area_id, mode)
    )

    pts = [
        row['geometry']['coordinates']
        for row in geojson['features']
    ]

    [[lrx, lry], [ulx, uly]] = get_bounding_box(pts)
    xsize = round((ulx - lrx) / resolution)
    ysize = round((uly - lry) / resolution)

    # inverse distance to a power
    gdal.Grid(outfile, json.dumps(geojson), zfield=field,
              algorithm="invdist:power=5:radius1=200:radius2=200:nodata=-1",
              outputSRS=f'EPSG:{SRS_ID}',
              outputBounds=[ulx, uly, lrx, lry], width=xsize, height=ysize)


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
