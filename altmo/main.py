import click

from altmo.commands.build import build
from altmo.commands.schema import schema
from altmo.commands.create_study_area import create_study_area


@click.group()
def cli():
    pass


cli.add_command(build)
cli.add_command(schema)
cli.add_command(create_study_area)


if __name__ == '__main__':
    cli()
