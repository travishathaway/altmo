import click

from altmo.data.schema import create_schema, remove_schema


@click.command()
@click.option('--drop', is_flag=True)
def schema(drop):
    if drop:
        if click.confirm('Are you sure you want to remove all tables and data?'):
            remove_schema()
    else:
        create_schema()
