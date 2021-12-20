import sys
import os
import json
from typing import List, Union

import click
from psycopg2.extras import register_hstore

from altmo.data.read import get_residence_composite_average_times
from altmo.data.decorators import psycopg2_cur
from altmo.settings import get_config, MODE_PEDESTRIAN, Config
from altmo.utils import (
    get_residence_composite_as_geojson,
    get_amenity_categories,
    get_available_amenity_categories
)
from altmo.validators import validate_mode, validate_study_area


EXPORT_TYPE_ALL = "all"
EXPORT_TYPE_SINGLE_RESIDENCE = "single_residence"


@get_config
def get_available_fields(config: Config):
    return ("all",) + get_available_amenity_categories(config.AMENITIES)


@get_config
def get_export_geojson(
    config: Config, cursor, study_area_id: int, mode: str, srs_id: str, properties: Union[tuple, None]
) -> dict:
    """Get the export geojson data as dict"""
    amenities = get_amenity_categories(config.AMENITIES)
    cols, data = get_residence_composite_average_times(
        cursor, study_area_id, mode, amenities, include_geojson=True, srs_id=srs_id
    )
    geojson = get_residence_composite_as_geojson(cols, data, props=properties)

    return geojson


def export_type_all(cursor, study_area_id: int, **kwargs: str) -> None:
    """writes the resulting GeoJSON to stdout"""
    mode = kwargs["mode"]
    srs_id = kwargs["srs_id"]
    properties = kwargs.get("properties")

    geojson = get_export_geojson(cursor, study_area_id, mode, srs_id, properties)

    click.echo(json.dumps(geojson))


def export_type_single_residence(cursor, study_area_id: int, **kwargs: str) -> None:
    """writes the resulting GeoJSON in a series of files identified by their `residence_id`"""
    mode = kwargs["mode"]
    srs_id = kwargs["srs_id"]
    export_dir = kwargs["export_dir"]

    properties = kwargs.get("properties")

    if os.path.exists(export_dir):
        click.echo("export directory already exists")
        sys.exit(1)

    os.mkdir(export_dir)
    register_hstore(cursor)  # turns on support for hstore
    geojson = get_export_geojson(cursor, study_area_id, mode, srs_id, properties)

    for feat in geojson.get("features", []):
        rez_id = feat["id"]
        export_loc = os.path.join(export_dir, f"{rez_id}.json")
        with open(export_loc, "w") as file:
            file.write(json.dumps(feat))


export_factory = {
    EXPORT_TYPE_ALL: export_type_all,
    EXPORT_TYPE_SINGLE_RESIDENCE: export_type_single_residence,
}


def process_properties(_, __, value) -> List[str]:
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


@click.command("export")
@click.argument("study_area", type=click.UNPROCESSED, callback=validate_study_area)
@click.argument("export_type", type=click.Choice(choices=tuple(export_factory.keys())))
@click.option(
    "-m",
    "--mode",
    default=MODE_PEDESTRIAN,
    type=click.UNPROCESSED,
    callback=validate_mode,
)
@click.option("-d", "--export-dir", default="export")
@click.option("-s", "--srs-id", default=3857)
@click.option("-p", "--properties", type=click.UNPROCESSED, callback=process_properties)
@psycopg2_cur()
def export(cursor, study_area, export_type, mode, export_dir, srs_id, properties):
    """
    Exports various formats of the analysis. Available formats are:

    - all (single file exported with defined properties included)
    - single_residence (exports individual geojson files for every residence into a directory)
    """
    # kwargs that we pass to all export functions
    kwargs = {
        "mode": mode,
        "export_dir": export_dir,
        "srs_id": srs_id,
        "properties": properties,
    }
    export_func = export_factory.get(export_type)

    export_func(cursor, study_area, **kwargs)
