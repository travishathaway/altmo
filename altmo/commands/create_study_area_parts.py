import json
import sys

import click
from psycopg2.errors import UniqueViolation

from altmo.data.decorators import psycopg2_cur
from altmo.data.read import get_study_area
from altmo.data.schema import STUDY_PARTS_TBL
from altmo.settings import PG_DSN


@click.command('csap')
@click.argument('study_area')
@click.argument('boundary',  type=click.File('r'))
@psycopg2_cur(PG_DSN)
def create_study_area_parts(cursor, study_area, boundary):
    """
    (Create Study Area Parts) import geojson boundary into our study_area_parts table
    """
    study_area_id, *_ = get_study_area(cursor, study_area)
    if not study_area_id:
        click.echo("study area not found")
        sys.exit(1)

    data = json.loads(boundary.read())
    study_area_parts = []

    try:
        srs_id = data['crs']['properties']['name'][-4:]

        for feat in data['features']:
            study_area_parts.append((
                feat['properties']['name'],
                feat['properties']['tags'],
                json.dumps(feat['geometry']),
                srs_id,
                study_area_id
            ))
    except (KeyError, IndexError):
        click.echo("Malformed GeoJSON")
        sys.exit(1)

    try:
        sql = f'''
            INSERT INTO {STUDY_PARTS_TBL} (name, description, geom, study_area_id)
            VALUES (%s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), %s), %s)
        '''
        for part in study_area_parts:
            cursor.execute(sql, part)
    except UniqueViolation:
        click.echo("Name already exists in database")
        sys.exit(1)
