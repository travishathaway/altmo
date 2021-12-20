from typing import Tuple, List


def get_matrix_request(
        residence: Tuple, batch: List[Tuple], costing: str = "auto"
) -> dict:
    """
    This returns the JSON serializable object that we pass to the matrix API.

    More information about the API request and response here:
        - https://valhalla.readthedocs.io/en/latest/api/matrix/api-reference/
    """
    res_lat, res_lng = residence
    sources = [{"lat": lat, "lon": lng} for lat, lng, *_ in filter(None, batch)]
    targets = [{"lat": res_lat, "lon": res_lng}]
    return {
        "sources": sources,
        "targets": targets,
        "costing": costing,
    }
