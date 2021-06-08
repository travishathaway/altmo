from multiprocessing import Pool
from typing import Tuple, List
from itertools import zip_longest

import click
import requests

from altmo.data.decorators import psycopg_context, psycopg2_cur
from altmo.data.read import (
    get_study_area,
    get_residence_amenity_straight_distance,
    get_study_area_residences
)
from altmo.data.write import add_amenity_residence_distance
from altmo.settings import PG_DSN, VALHALLA_SERVER

VALHALLA_API_MATRIX_ENDPOINT = f'{VALHALLA_SERVER}/sources_to_targets'
MODE_PEDESTRIAN = 'pedestrian'
MODE_BICYCLE = 'bicycle'


@click.command('network')
@click.argument('study_area', type=str)
@click.option('-p', '--processes', type=int)
@click.option('-m', '--mode', type=str, default='pedestrian')
@psycopg2_cur(PG_DSN)
def network_distances(cursor, study_area, processes, mode):
    """calculate network distances between residences and amenities"""
    study_area_id, *_ = get_study_area(cursor, study_area)
    residences = get_study_area_residences(cursor, study_area_id)

    # Organizes residence ids in to batches which the various processes will process
    batches = zip_longest(*(iter(residences),) * (len(residences) // processes))

    with Pool(processes) as p:
        if mode == MODE_PEDESTRIAN:
            p.map(set_network_distances_pedestrian, batches)
        elif mode == MODE_BICYCLE:
            p.map(set_network_distances_bicycle, batches)
        else:
            click.echo('invalid option supply for -m|--mode')


def set_network_distances(residence_ids: list, mode: str) -> None:
    """set network distance for the provided residence ids"""
    with psycopg_context(PG_DSN) as cursor:
        residence_ids = [x for x in residence_ids if x]
        for res_id, res_lng, res_lat in residence_ids:
            if res_id:
                am_dist = get_residence_amenity_straight_distance(cursor, res_id)
                if am_dist:
                    matrix_req = get_matrix_request((res_lat, res_lng), am_dist, costing=mode)
                    resp = requests.post(VALHALLA_API_MATRIX_ENDPOINT, json=matrix_req)
                    new_rows = []

                    for [distance, *_], (amenity_id, *_) in zip(resp.json().get('sources_to_targets', []), am_dist):
                        new_rows.append((distance['distance'], distance['time'], amenity_id, res_id, mode))
                    add_amenity_residence_distance(cursor, new_rows)


def set_network_distances_bicycle(residence_ids: list) -> None:
    """set network distance for bicycle"""
    set_network_distances(residence_ids, MODE_BICYCLE)


def set_network_distances_pedestrian(residence_ids: list) -> None:
    """set network distance for pedestrian"""
    set_network_distances(residence_ids, MODE_PEDESTRIAN)


def get_matrix_request(residence: Tuple, batch: List[Tuple], costing: str = 'auto') -> dict:
    """
    This returns the JSON serializable object that we pass to the matrix API.

    More information about the API request and response here:
        - https://valhalla.readthedocs.io/en/latest/api/matrix/api-reference/
    """
    sources = [{'lat': x[2], 'lon': x[1]} for x in batch if x]
    targets = [{'lat': residence[0], 'lon': residence[1]}]
    return {
        'sources': sources,
        'targets': targets,
        'costing': costing,
    }
