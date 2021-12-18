import sys

import click
from psycopg2.errors import DuplicateTable

from altmo.data.schema import create_schema, remove_schema


@click.command()
@click.option("--drop", is_flag=True)
def schema(drop):
    """Adds or removes tables from our database necessary for running the analysis"""
    if drop:
        if click.confirm("Are you sure you want to remove all tables and data?"):
            remove_schema()
    else:
        try:
            create_schema()
        except DuplicateTable:
            click.echo("Tables in schema already exist")
            sys.exit(1)
