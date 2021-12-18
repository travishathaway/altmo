import json
import sys

import click
from psycopg2.errors import UniqueViolation

from altmo.data.decorators import psycopg2_cur
from altmo.data import write


@click.command("csa")
@click.argument("boundary", type=click.File("r"))
@click.argument("name", type=str)
@click.argument("description", type=str)
@click.argument("srs_id", type=int)
@psycopg2_cur()
def create_study_area(cursor, boundary, name, description, srs_id):
    """
    (Create Study Area) import geojson boundary into our study_areas table
    """
    try:
        data = json.loads(boundary.read())
        data["name"] = name
        data["description"] = description
        write.create_study_area(cursor, data, srs_id)
    except (KeyError, IndexError, json.decoder.JSONDecodeError):
        click.echo("Malformed GeoJSON")
        sys.exit(1)
    except UniqueViolation:
        click.echo("Name already exists in database")
        sys.exit(1)
