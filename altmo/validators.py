from typing import List

import click

from altmo.settings import (
    MODE_PEDESTRIAN, MODE_BICYCLE, PARALLEL_HIGH, PARALLEL_LOW,
    config_manager
)
from altmo.data.decorators import psycopg2_context
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


def validate_study_area(_, __, value) -> int:
    """validates study_area parameter and returns the study_area_id"""
    with config_manager() as config:
        with psycopg2_context(conn_info=config.PG_DSN) as cursor:
            study_area_id, *_ = get_study_area(cursor, value)

    if not study_area_id:
        raise click.BadParameter(f'Study area "{value}" not found.')

    return study_area_id


def validate_parallel(_, __, value) -> str:
    """validates parallel parameter"""
    available_choices = {PARALLEL_LOW, PARALLEL_HIGH}

    if value not in available_choices:
        raise click.BadParameter(f'value for -p|--parallel option must be one of {",".join(available_choices)}')

    return value
