"""
Holds a config data fixture that is stored as a dictionary
"""

CONFIG_DATA = {
    'PG_DSN': 'dbname=database user=db_user host=localhost port=5432 password=password',
    'VALHALLA_SERVER': 'http://localhost:8003',
    'TBL_PREFIX': 'altmo_',
    'SRS_ID': 3857,
    'AMENITIES': {
        'include_natural_amenities': True,
        'categories': {
            'school': {
                'kindergarten': {'weight': 0.15},
                'childcare': {'weight': 0.2},
                'university': {'weight': 0.175},
                'music_school': {'weight': 0.075},
                'driving_school': {'weight': 0.075},
                'college': {'weight': 0.0125},
                'research_institute': {'weight': 0.075},
                'school': {'weight': 0.2}
            },
            'shopping': {
                'marketplace': {'weight': 0.111},
                'hairdresser': {'weight': 0.111},
                'clothes': {'weight': 0.111},
                'books': {'weight': 0.111},
                'florist': {'weight': 0.111},
                'optician': {'weight': 0.111},
                'furniture': {'weight': 0.111},
                'sports': {'weight': 0.111},
                'second_hand': {'weight': 0.112}
            },
            'groceries': {
                'supermarket': {'weight': 0.75},
                'bakery': {'weight': 0.25},
                'butcher': {'weight': 0}
            },
            'administrative': {
                'townhall': {'weight': 0.25},
                'police': {'weight': 0.2},
                'bank': {'weight': 0.25},
                'post_office': {'weight': 0.2},
                'post_box': {'weight': 0.1}
            },
            'health': {
                'doctors': {'weight': 0.2},
                'hospital': {'weight': 0.15},
                'nursing_home': {'weight': 0.1},
                'veterinary': {'weight': 0.1},
                'pharmacy': {'weight': 0.2},
                'dentist': {'weight': 0.15},
                'clinic': {'weight': 0.1}
            },
            'community': {
                'community_centre': {'weight': 0.25},
                'social_facility': {'weight': 0.25},
                'library': {'weight': 0.25},
                'place_of_worship': {'weight': 0.25}
            },
            'outing_destination': {
                'pub': {'weight': 0.1},
                'cafe': {'weight': 0.1},
                'theatre': {'weight': 0.1},
                'nightclub': {'weight': 0.1},
                'bar': {'weight': 0.1},
                'ice_cream': {'weight': 0.1},
                'events_venue': {'weight': 0.1},
                'cinema': {'weight': 0.1},
                'restaurant': {'weight': 0.1},
                'fast_food': {'weight': 0.1}
            },
            'nature': {
                'allotment': {'weight': 0.1},
                'cemetery': {'weight': 0.1},
                'park': {'weight': 0.3},
                'forest': {'weight': 0.2},
                'sports': {'weight': 0.3}
            }
        }
    }
}
