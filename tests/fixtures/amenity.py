from __future__ import annotations

import json
import random

AMENITY_CATEGORY_PAIRS = [
    ('police', 'administrative'),
    ('post_box', 'administrative'),
    ('bank', 'administrative'),
    ('post_office', 'administrative'),
    ('townhall', 'administrative'),
    ('library', 'community'),
    ('community_centre', 'community'),
    ('place_of_worship', 'community'),
    ('social_facility', 'community'),
    ('supermarket', 'groceries'),
    ('bakery', 'groceries'),
    ('butcher', 'groceries'),
    ('doctors', 'health'),
    ('nursing_home', 'health'),
    ('dentist', 'health'),
    ('hospital', 'health'),
    ('clinic', 'health'),
    ('veterinary', 'health'),
    ('pharmacy', 'health'),
    ('cemetery', 'nature'),
    ('allotment', 'nature'),
    ('forest', 'nature'),
    ('park', 'nature'),
    ('sports', 'nature'),
    ('bar', 'outing_destination'),
    ('events_venue', 'outing_destination'),
    ('restaurant', 'outing_destination'),
    ('cinema', 'outing_destination'),
    ('fast_food', 'outing_destination'),
    ('pub', 'outing_destination'),
    ('nightclub', 'outing_destination'),
    ('ice_cream', 'outing_destination'),
    ('theatre', 'outing_destination'),
    ('cafe', 'outing_destination'),
    ('college', 'school'),
    ('driving_school', 'school'),
    ('music_school', 'school'),
    ('school', 'school'),
    ('childcare', 'school'),
    ('university', 'school'),
    ('kindergarten', 'school'),
    ('optician', 'shopping'),
    ('florist', 'shopping'),
    ('clothes', 'shopping'),
    ('hairdresser', 'shopping'),
    ('books', 'shopping'),
    ('second_hand', 'shopping'),
    ('marketplace', 'shopping'),
    ('furniture', 'shopping'),
]


def get_residence_composite_average_times() -> list[tuple]:
    """
    Returns something that looks like what `altmo.data.read.get_residence_composite_average_times`
    would return
    """
    geom = {
        "type": "Feature",
        "id": 2809,
        "geometry": {
            "type": "Point",
            "crs": {
                "type": "name",
                "properties": {
                    "name": "EPSG:4236"
                }
            },
            "coordinates": [
                18.107006528,
                59.351117532
            ]
        }
    }

    random_average_values = tuple(round(random.random() * 1000, 4) for _ in range(9))

    return [
        (idx, ) + random_average_values + (json.dumps({**geom, **{'id': idx}}), )
        for idx in range(1, 11, 1)
    ]
