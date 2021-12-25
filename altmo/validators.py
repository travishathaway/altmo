from typing import List

import click

from altmo.settings import MODE_PEDESTRIAN, MODE_BICYCLE
from altmo.data.decorators import psycopg2_cur
from altmo.data.read import get_study_area


def validate_mode(_, __, value) -> List[str]:
    """validates mode parameter"""
    if value:
        available_choices = (MODE_BICYCLE, MODE_PEDESTRIAN)
        if value not in available_choices:
            raise click.BadParameter(
                f'"{value}" is not valid. Choices are {", ".join(available_choices)}'
            )

        return value or None


@psycopg2_cur()
def validate_study_area(cursor, _, __, value) -> int:
    """validates study_area parameter and returns the study_area_id"""
    study_area_id, *_ = get_study_area(cursor, value)

    if not study_area_id:
        raise click.BadParameter(f'Study area "{value}" not found.')

    return study_area_id


OUT_DB = 'db'
OUT_STDOUT = 'stdout'
OUT_CSV = 'csv'


def validate_out(_, __, value) -> List[str]:
    """validates mode parameter"""
    if value:
        available_choices = (OUT_DB, OUT_CSV, OUT_STDOUT)
        if value not in available_choices:
            raise click.BadParameter(
                f'"{value}" is not valid. Choices are {", ".join(available_choices)}'
            )

        return value or None

