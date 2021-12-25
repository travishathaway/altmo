import json
from itertools import islice
from typing import List, Dict, Iterable

from .errors import AltmoConfigError, CONFIG_ERROR_MSG


def get_amenities_from_config(config_data: Dict[str, Dict[str, Dict]]) -> List[str]:
    """
    Retrieves the OSM amenities from our config_data object.

    We intentionally exclude "nature" as this category is handled specially.
    """
    try:
        config_amenities: List[str] = []
        amenity_config = config_data["categories"]

        for category, amenities in amenity_config.items():
            if category != "nature":
                for amenity in amenities.keys():
                    config_amenities.append(amenity)
        return config_amenities
    except KeyError:
        raise AltmoConfigError(CONFIG_ERROR_MSG)


def get_amenity_category_map(config_data: Dict[str, Dict]) -> Dict:
    """returns a mapping we can use to look up a category for an amenity"""
    try:
        return {
            amenity: category
            for category, amenities in config_data["categories"].items()
            for amenity in amenities
        }
    except KeyError:
        raise AltmoConfigError(CONFIG_ERROR_MSG)


def get_category_amenity_keys(categories: Dict[str, Dict]) -> List[str]:
    """
    Returns category and amenity combined together as a single str `{category}_{amenity}`
    """
    try:
        return [
            f"{category}_{amenity}"
            for category, amenities in categories.items()
            for amenity in amenities.keys()
        ]
    except KeyError:
        raise AltmoConfigError(CONFIG_ERROR_MSG)


def get_amenity_category_weights(
    config_data: Dict[str, Dict[str, Dict]]
) -> Dict[str, float]:
    """
    Returns amenity category and amenity name combined together which can be used in a pivot table query
    """
    try:
        amenity_categories = config_data["categories"]
        return {
            f"{category}_{amenity}": weight
            for category, amenities in amenity_categories.items()
            for amenity, weight in amenities.items()
        }
    except KeyError:
        raise AltmoConfigError(CONFIG_ERROR_MSG)


def get_amenity_categories(config_data: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    safely returns the configured amenities. If they are not there then a AltmoConfigError is thrown
    """
    try:
        return config_data["categories"]
    except KeyError:
        raise AltmoConfigError(CONFIG_ERROR_MSG)


def get_residence_composite_as_dicts(cols: tuple, data: List[tuple]) -> List[Dict]:
    """returns the residence composite results as a list of dictionaries"""
    ret_list = []

    for row in data:
        row_data = {}
        for key, val in zip(cols, row):
            if isinstance(val, str):
                row_data[key] = json.loads(val)
            else:
                row_data[key] = round(float(val), ndigits=5)
        ret_list.append(row_data)

    return ret_list


def get_residence_composite_as_geojson(
    cols: tuple, data: List[tuple], props: tuple = None
) -> Dict:
    """
    Gets the residence composite results as geojson dict.

    :param cols: Columns for the corresponding rows in `data`
    :param data: Rows of residence composite  data
    :param props: Properties to include in "properties" section of each entry.
                  Passing in `None` (default) includes everything.
                  Pass in a empty tuple for no properties.
    """
    geojson_data = {"type": "FeatureCollection", "features": []}
    if props is None:
        props = [m for m in cols if m not in ("geom", "residence_id")]

    for row in data:
        row_data = dict(zip(cols, row))
        geojson_data["features"].append(
            {
                "type": "Feature",
                "id": row_data["residence_id"],
                "geometry": json.loads(row_data["geom"]),
                "properties": {
                    x: round(float(y), ndigits=5)
                    for x, y in row_data.items()
                    if x in props
                },
            }
        )

    return geojson_data


def get_available_amenity_categories(config_data: Dict[str, Dict]) -> tuple:
    """
    Reads the config_data dictionary and returns all currently available amenity categories

    If we are unable to find the data an AltmoConfigError is raise.

    :raises: AltmoConfigError
    """
    try:
        return tuple([cat for cat in config_data["categories"].keys()])
    except KeyError:
        raise AltmoConfigError(CONFIG_ERROR_MSG)


def grouper(iterable: Iterable, size: int) -> Iterable:
    """
    >>> list(grouper(3, 'ABCDEFG'))
    [['A', 'B', 'C'], ['D', 'E', 'F'], ['G']]
    """
    iterable = iter(iterable)
    return iter(lambda: list(islice(iterable, size)), [])
