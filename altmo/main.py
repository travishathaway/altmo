import click

from altmo.commands.build import build
from altmo.commands.schema import schema
from altmo.commands.create_study_area import create_study_area
from altmo.commands.create_study_area_parts import create_study_area_parts
from altmo.commands.network_distances import network_distances
from altmo.commands.straight_distances import straight_distance
from altmo.commands.calculate_zscores import calculate_zscores


@click.group()
def cli():
    pass


cli.add_command(build)
cli.add_command(schema)
cli.add_command(create_study_area)
cli.add_command(create_study_area_parts)
cli.add_command(network_distances)
cli.add_command(straight_distance)
cli.add_command(calculate_zscores)


if __name__ == '__main__':
    cli()
