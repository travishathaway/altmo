import sys
import math
from typing import List, Tuple

import click
import gdal
import ogr
import osr
import numpy as np

from altmo.settings import PG_DSN, SRS_ID
from altmo.data.decorators import psycopg2_cur
from altmo.data.read import get_study_area

READ_PTS_SQL = '''
    SELECT
        ST_X(geom) as lat, ST_Y(geom) as long,
        all_average_time,
        administrative_average_time, community_average_time, groceries_average_time,
        health_average_time, nature_average_time, outing_destination_average_time, school_average_time,
        shopping_average_time
    FROM
        "residence_amenity_distance_standardized_categorized" c
    JOIN
        residences r
    ON
        r.id = c.residence_id
    WHERE
        c.mode = %s
    AND
        r.study_area_id = %s
'''


@click.command('raster_rough')
@click.argument('study_area')
@click.argument('outfile')
@click.option('-m', '--mode', default='walking')
@click.option('-h', '--pixel-height', default=100)
@click.option('-w', '--pixel-width', default=100)
@psycopg2_cur(PG_DSN)
def raster_rough(cursor, study_area, outfile, mode, pixel_height, pixel_width) -> None:
    """Generates raster data from database"""
    study_area_id, *_ = get_study_area(cursor, study_area)
    if not study_area_id:
        click.echo("study area not found")
        sys.exit(1)

    cursor.execute(READ_PTS_SQL, (mode, study_area_id))
    res = cursor.fetchall()

    route_pts = [
        (x, y)
        for x, y, *_ in res
    ]

    route_pts_values = {
        (x, y): value
        for x, y, value, *_ in res
    }

    bound_min, bound_max = get_bounding_box(route_pts)

    p_width = pixel_width
    p_height = pixel_height

    rstr_orig = bound_min
    new_rstr_fn = outfile

    density_arr = get_density(route_pts, route_pts_values, p_height, p_width)
    array2raster(new_rstr_fn, rstr_orig, p_width, p_height, density_arr)


def array2raster(new_raster_fn, raster_origin, pixel_width, pixel_height, array):
    cols = array.shape[1]
    rows = array.shape[0]
    origin_x = raster_origin[0]
    origin_y = raster_origin[1]

    driver = gdal.GetDriverByName('GTiff')
    out_raster = driver.Create(new_raster_fn, cols, rows, 1, gdal.GDT_Int16)
    out_raster.SetGeoTransform((origin_x, pixel_width, 0, origin_y, 0, pixel_height))
    outband = out_raster.GetRasterBand(1)
    outband.WriteArray(array)
    out_raster_srs = osr.SpatialReference()
    out_raster_srs.ImportFromEPSG(SRS_ID)
    out_raster.SetProjection(out_raster_srs.ExportToWkt())
    outband.FlushCache()


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


def get_density(pts: List[Tuple], values: dict, p_height: float, p_width: float) -> np.ndarray:
    """
    Returns a two dimensional array with average values in cells.
    """
    bound_min, bound_max = get_bounding_box(pts)
    min_x, min_y = bound_min
    max_x, max_y = bound_max
    rstr_cells_width = math.ceil((max_x - min_x) / p_width)
    rstr_cells_height = math.ceil((max_y - min_y) / p_height)

    cell_vals = {}

    for x, y in pts:
        row = math.ceil((y - min_y) / p_height) - 1
        col = math.ceil((x - min_x) / p_width) - 1
        if not cell_vals.get((row, col)):
            cell_vals[(row, col)] = [(x, y)]
        else:
            cell_vals[(row, col)].append((x, y))

    d_vals = np.full((rstr_cells_height, rstr_cells_width), 0, dtype=np.int)

    for x, y in pts:
        row = math.ceil((y - min_y) / p_height) - 1
        col = math.ceil((x - min_x) / p_width) - 1
        cell = cell_vals[(row, col)]
        average = sum(values[(x, y)] for x, y in cell) / len(cell)
        d_vals[row][col] = average

    return d_vals
