from typing import List, Dict

from .errors import AltmoConfigError, CONFIG_ERROR_MSG


def get_amenities_from_config(config_data: Dict[str, Dict[str, Dict]]) -> List[str]:
    """
    Retrieves the OSM amenities from our config_data object.

    We intentionally exclude "nature" as this category is handled specially.
    """
    try:
        config_amenities: List[str] = []
        amenity_config = config_data['amenities']['categories']

        for category, amenities in amenity_config.items():
            if category != 'nature':
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
            for category, amenities in config_data['amenities']['categories'].items()
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
            f'{category}_{amenity}'
            for category, amenities in categories.items()
            for amenity in amenities.keys()
        ]
    except KeyError:
        raise AltmoConfigError(CONFIG_ERROR_MSG)


def get_amenity_category_weights(config_data: Dict[str, Dict[str, Dict]]) -> Dict[str, float]:
    """
    Returns amenity category and amenity name combined together which can be used in a pivot table query
    """
    try:
        amenity_categories = config_data['amenities']['categories']
        return {
            f'{category}_{amenity}': weight
            for category, amenities in amenity_categories.items()
            for amenity, weight in amenities.items()
        }
    except KeyError:
        raise AltmoConfigError(CONFIG_ERROR_MSG)
