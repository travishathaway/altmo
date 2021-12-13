import sys
import json
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
@click.option('-m', '--mode', type=str, default=MODE_PEDESTRIAN)
@click.option('-c', '--category', type=str)
@click.option('-n', '--name', type=str)
@psycopg2_cur(PG_DSN)
def network_distances(cursor, study_area, processes, mode, category, name):
    """calculate network distances between residences and amenities"""
    study_area_id, *_ = get_study_area(cursor, study_area)
    if not study_area_id:
        click.echo("study area not found")
        sys.exit(1)
    residences = get_study_area_residences(cursor, study_area_id)

    # Organizes residence ids in to batches which the various processes will process
    # Size of batches = <number_of_residences> / <number_of_processes>
    batches = zip_longest(*(iter(residences),) * (len(residences) // processes))
    argument_batches = [(batch, category, name) for batch in batches]

    with Pool(processes) as p:
        if mode == MODE_PEDESTRIAN:
            p.map(set_network_distances_pedestrian, argument_batches)
        elif mode == MODE_BICYCLE:
            p.map(set_network_distances_bicycle, argument_batches)
        else:
            click.echo('invalid option supplied for -m|--mode')


def set_network_distances(residence_ids: list, mode: str, category: str = None, name: str = None) -> None:
    """set network distance for the provided residence ids"""
    with psycopg_context(PG_DSN) as cursor:
        residence_ids = [x for x in residence_ids if x]
        for res_id, res_lng, res_lat in residence_ids:
            if res_id:
                set_residence_amenity_network_distances(
                    cursor, res_id, res_lng, res_lat, mode, category=category, name=name
                )


def set_residence_amenity_network_distances(
        cursor, residence_id: int, residence_lng: float, residence_lat: float, mode: str,
        category: str = None, name: str = None) -> None:
    """
    Provided a residence, call the Valhalla API in batches of 50 and then
    create new database records for the nearest amenity distances.
    """
    am_dist = get_residence_amenity_straight_distance(cursor, residence_id, category=category, name=name)
    if am_dist:
        batches = zip_longest(*(iter(am_dist),) * 50)  # split these records up into groups of 50

        for batch in batches:
            matrix_req = get_matrix_request((residence_lat, residence_lng), batch, costing=mode)
            resp = requests.post(VALHALLA_API_MATRIX_ENDPOINT, json=matrix_req)
            new_rows = []

            for [distance, *_], (amenity_id, *_) in zip(resp.json().get('sources_to_targets', []), batch):
                new_rows.append((distance['distance'], distance['time'], amenity_id, residence_id, mode))
            try:
                add_amenity_residence_distance(cursor, new_rows)
            except Exception as exc:
                # Inefficient, but allows us to recover from errors when the process fails
                click.echo(exc)
                click.echo(json.dumps(matrix_req))
                continue
        cursor.connection.commit()


def set_network_distances_bicycle(arguments: tuple) -> None:
    """set network distance for bicycle"""
    residence_ids, category, name = arguments
    set_network_distances(residence_ids, MODE_BICYCLE, category=category, name=name)


def set_network_distances_pedestrian(arguments: tuple) -> None:
    """set network distance for pedestrian"""
    residence_ids, category, name = arguments
    set_network_distances(residence_ids, MODE_PEDESTRIAN, category=category, name=name)


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
