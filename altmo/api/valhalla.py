from typing import Tuple, List

import requests

API_ENDPOINT = 'http://localhost:8002/route'
MATRIX_ENDPOINT = 'http://localhost:8002/sources_to_targets'


def get_matrix_request(poi: Tuple, batch: List[Tuple], costing: str = 'auto') -> dict:
    """
    This returns the JSON serializable object that we pass to the matrix API.

    More information about the API request and response here:
        - https://valhalla.readthedocs.io/en/latest/api/matrix/api-reference/
    """
    sources = [{'lat': x[2], 'lon': x[3]} for x in batch if x]
    targets = [{'lat': poi[0], 'lon': poi[1]}]
    return {
        'sources': sources,
        'targets': targets,
        'costing': costing
    }
