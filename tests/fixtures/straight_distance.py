"""
Holds code for mocking the `residence_amenity_distances_straight` table
"""
import random

from altmo.data.types import StraightDistanceRow

from tests.fixtures.valhalla import VALHALLA_MATRIX_RESPONSE


def get_lat() -> float:
    return random.randint(-90_000_000, 90_000_000) / 1_000_000


def get_lng() -> float:
    return random.randint(-180_000_000, 180_000_000) / 1_000_000


RESIDENCES = [
    (id_, get_lat(), get_lng())
    for id_ in range(1, 11, 1)
]

AMENITIES = [
    (id_, get_lat(), get_lng())
    for id_ in range(1, len(VALHALLA_MATRIX_RESPONSE['sources_to_targets'][0]) + 1, 1)
]

STRAIGHT_DISTANCE = [
    StraightDistanceRow(res_id, amt_id, res_lat, res_lng, amt_lat, amt_lng)
    for res_id, res_lat, res_lng in RESIDENCES
    for amt_id, amt_lat, amt_lng in AMENITIES
]
