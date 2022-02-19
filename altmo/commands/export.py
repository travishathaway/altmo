from __future__ import annotations

import csv
import dataclasses
import json
import os
from collections import Sequence
from pathlib import Path
from typing import Literal

import click

from altmo.data.decorators import psycopg2_cur, register_hstore
from altmo.data.read import get_residence_composite_average_times
from altmo.settings import get_config, MODE_PEDESTRIAN, Config
from altmo.utils import (
    get_residence_composite_as_geojson,
    get_amenity_categories,
    get_available_amenity_categories
)
from altmo.validators import validate_mode, validate_study_area

EXPORT_TYPE_ALL = "all"
EXPORT_TYPE_SINGLE_RESIDENCE = "single_residence"


@dataclasses.dataclass
class ExportConfig:
    study_area_id: int
    mode: str
    export_dir: str
    export_type: str
    srs_id: str
    properties: list[str]
    file_format: Literal['csv', 'json']
    file_name: Path


@get_config
def get_available_fields(config: Config) -> tuple[str]:
    return ("all",) + get_available_amenity_categories(config.AMENITIES)


@get_config
def get_export_data(config: Config, cursor, export_config: ExportConfig) -> tuple[Sequence, list[tuple]]:
    """Get the export geojson data as dict"""
    amenities = get_amenity_categories(config.AMENITIES)
    include_geojson = export_config.file_format == 'json'

    cols, data = get_residence_composite_average_times(
        cursor, export_config.study_area_id, export_config.mode, amenities,
        include_geojson=include_geojson, srs_id=export_config.srs_id
    )

    return cols, data


def write_export_csv(columns: Sequence, data: list[tuple], export_config: ExportConfig) -> None:
    with open(export_config.file_name, 'w') as fp:
        csvwriter = csv.writer(fp)
        csvwriter.writerow(columns)
        for row in data:
            csvwriter.writerow(row)


def write_geojson_to_stdout(columns: Sequence, data: list[tuple], export_config: ExportConfig):
    geojson = get_residence_composite_as_geojson(columns, data, export_config.properties)
    click.echo(json.dumps(geojson))


format_funcs = {
    'json': write_geojson_to_stdout,
    'csv': write_export_csv
}


def export_type_all(cursor, export_config: ExportConfig) -> None:
    """writes the resulting GeoJSON to stdout"""
    format_func = format_funcs[export_config.file_format]
    cols, data = get_export_data(cursor, export_config)
    format_func(cols, data, export_config)


@register_hstore
def export_type_single_residence(cursor, export_config: ExportConfig) -> None:
    """
    Writes the resulting GeoJSON in a series of files identified by their `residence_id`
    """
    cols, data = get_export_data(cursor, export_config)
    geojson = get_residence_composite_as_geojson(cols, data, export_config.properties)

    for feat in geojson.get("features", []):
        rez_id = feat["id"]
        export_loc = os.path.join(export_config.export_dir or '.', f"{rez_id}.json")
        with open(export_loc, "w") as file:
            file.write(json.dumps(feat))


export_factory = {
    EXPORT_TYPE_ALL: export_type_all,
    EXPORT_TYPE_SINGLE_RESIDENCE: export_type_single_residence,
}


def process_properties(_, __, value) -> list[str]:
    """returns properties param as a list, parses from a comma separated string"""
    if value:
        value_list = [val.strip() for val in value.split(",")]
        available_fields = get_available_fields()
        for val in value_list:
            if val not in available_fields:
                raise click.BadParameter(
                    f'"{val}" is not available. Choices are {", ".join(available_fields)}'
                )
        return value_list or None


def validate_file_format(_, __, value) -> str:
    if value not in ('csv', 'json'):
        raise click.BadParameter(f'{value} is not a valid choice. Choices are "csv" or "json"')
    return value


@click.command("export")
@click.argument("study_area_id", type=click.UNPROCESSED, callback=validate_study_area)
@click.argument("export_type", type=click.Choice(choices=tuple(export_factory.keys())))
@click.option(
    "-m",
    "--mode",
    default=MODE_PEDESTRIAN,
    type=click.UNPROCESSED,
    callback=validate_mode,
)
@click.option("-d", "--export-dir", type=click.Path(exists=True, dir_okay=True, file_okay=False))
@click.option("-s", "--srs-id", default=3857)
@click.option("-p", "--properties", type=click.UNPROCESSED, callback=process_properties)
@click.option("-f", "--file-format", type=click.UNPROCESSED, callback=validate_file_format, default='json')
@click.option("-n", "--file-name", type=click.Path(exists=False))
@psycopg2_cur()
def export(cursor, **kwargs):
    """
    Exports various formats of the analysis. Available formats are:

    - all (single file exported with defined properties included)
    - single_residence (exports individual geojson files for every residence into a directory)
    """
    export_config = ExportConfig(**kwargs)
    export_func = export_factory.get(export_config.export_type)
    export_func(cursor, export_config)
