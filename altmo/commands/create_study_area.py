import json
import sys

import click
import psycopg2

from altmo.data.decorators import psycopg2_cur
from altmo.settings import PG_DSN


@click.command('csa')
@click.argument('boundary',  type=click.File('r'))
@click.argument('name', type=str)
@click.argument('description', type=str)
@click.argument('srs_id', type=int)
@psycopg2_cur(PG_DSN)
def create_study_area(cursor, boundary, name, description, srs_id):
    """
    (Create Study Area) import geojson boundary into our study_areas table
    """
    data = json.loads(boundary.read())

    try:
        sql = f'''
            INSERT INTO study_areas (name, description, geom)
            VALUES (%s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), {srs_id}))
        '''
        features = data['features']
        feat = features[0]
        geometry = feat['geometry']
    except (KeyError, IndexError):
        click.echo("Malformed GeoJSON")
        sys.exit(1)

    try:
        cursor.execute(sql, (name, description, json.dumps(geometry)))
    except psycopg2.errors.UniqueViolation:
        click.echo("Name already exists in database")
        sys.exit(1)
