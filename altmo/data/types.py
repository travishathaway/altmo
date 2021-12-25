from collections import namedtuple
from dataclasses import dataclass


@dataclass
class Point:
    id: int
    lat: float
    lng: float

    def __hash__(self):
        return hash((self.id, self.lat, self.lng))


StraightDistanceRow = namedtuple('StraightDistanceRow', [
    'residence_id',
    'amenity_id',
    'residence_lat',
    'residence_lng',
    'amenity_lat',
    'amenity_lng'
])
