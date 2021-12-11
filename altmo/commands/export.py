import sys
import os
import json

import click
from psycopg2.extras import register_hstore

from altmo.data.read import (
    get_study_area,
    get_residence_points_time_zscore_geojson,
    get_residence_points_all_web_geojson,
    get_residences
)
from altmo.data.decorators import psycopg2_cur
from altmo.settings import PG_DSN


EXPORT_TYPES = (
    'study_area_parts',  # Aggregated by city parts and includes all categories
    'residences'
)

SINGLE_RESIDENCE_DATA_FIELDS = (
    'administrative_average_time',
    'community_average_time',
    'groceries_average_time',
    'health_average_time',
    'nature_average_time',
    'outing_destination_average_time',
    'school_average_time',
    'shopping_average_time',
    'all_average_time',
)


@click.command('export')
@click.argument('study_area', type=str)
@click.argument('export_type', type=str)
@click.option('-m', '--mode', default='pedestrian')
@click.option('-d', '--export-dir', default='export')
@psycopg2_cur(PG_DSN)
def export(cursor, study_area, export_type, mode, export_dir):
    """Exports various formats of the analysis"""
    study_area_id, *_ = get_study_area(cursor, study_area)
    if not study_area_id:
        click.echo("study area not found")
        sys.exit(1)

    if export_type == 'all':
        click.echo(get_residence_points_time_zscore_geojson(cursor, study_area_id, mode))
    elif export_type == 'all_web':
        click.echo(get_residence_points_all_web_geojson(cursor, study_area_id, mode))
    elif export_type == 'single_residences':
        if os.path.exists(export_dir):
            click.echo('directory already exists')
            sys.exit(1)

        os.mkdir(export_dir)

        register_hstore(cursor)  # turns on support for hstore
        residences = get_residences(cursor, study_area_id, with_stats=True)
        rez_dict = {}
        for rez in residences:
            rez_id = rez[0]
            rez_mode = rez[15]
            data = dict(zip(SINGLE_RESIDENCE_DATA_FIELDS, rez[6:15]))

            if not rez_dict.get(rez_id):
                rez_dict[rez_id] = {'properties': {}}

            rez_dict[rez_id]['properties'][rez_mode] = data
            rez_dict[rez_id]['id'] = rez_id
            rez_dict[rez_id]['properties']['tags'] = rez[2]
            rez_dict[rez_id]['properties']['house_number'] = rez[3]

        # second pass to actually write json files to disk
        for rez_id, data in rez_dict.items():
            export_loc = os.path.join(export_dir, f'{rez_id}.json')
            with open(export_loc, 'w') as f:
                f.write(json.dumps(data))
