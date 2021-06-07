import math
from multiprocessing import Pool

import click

from altmo.data.decorators import psycopg_context, psycopg2_cur
from altmo.data.read import (
    get_study_area,
    get_residence_amenity_straight_distance, get_residence_amenity_straight_distance_count
)
from altmo.settings import PG_DSN


@click.command('network')
@click.argument('study_area', type=str)
@click.option('-p', '--processes', type=int)
@psycopg2_cur(PG_DSN)
def network_distances(cursor, study_area, processes):
    """calculate network distances between residences and amenities"""
    # TODO: I need to find a way to make this run in parallel
    study_area_id, *_ = get_study_area(cursor, study_area)
    res_am_straight_count = get_residence_amenity_straight_distance_count(cursor, study_area_id)

    batch_size = math.ceil(res_am_straight_count / processes)
    batches = [(x, x + batch_size, study_area_id) for x in range(0, processes * batch_size, batch_size)]

    with Pool(processes) as p:
        p.map(_map_set_network_distances, batches)


def _map_set_network_distances(*args):
    start, stop, study_area_id = args[0]
    set_network_distances(start, stop, study_area_id)


def set_network_distances(start: int, stop: int, study_area_id: int) -> None:
    """given the range, creates new records for network distance"""
    with psycopg_context(PG_DSN) as cursor:
        res_am_dist = get_residence_amenity_straight_distance(cursor, study_area_id, start, stop)

        print(res_am_dist[0])

