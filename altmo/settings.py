import os
import sys
from dataclasses import dataclass
from functools import wraps

import yaml
import yaml.parser

# Static variables
DEFAULT_CONFIG_FILE_NAME = "altmo-config.yml"
MODE_PEDESTRIAN = "pedestrian"
MODE_BICYCLE = "bicycle"
MODE_AUTO = "auto"


@dataclass
class Config:
    """Holds configuration which is loaded from config file"""
    TBL_PREFIX: str = None
    SRS_ID: int = None
    PG_DSN: str = None
    VALHALLA_SERVER: str = None
    AMENITIES: dict = None

    def __post_init__(self):
        self._config_loaded = False

    def _load_config(self):
        config_file = get_config_file(DEFAULT_CONFIG_FILE_NAME)
        try:
            config_file_data = parse_config_file(config_file)
        except (FileNotFoundError, yaml.parser.ParserError):
            sys.stderr.write(
                f"Unable to parse {config_file} "
                "Please create this file and set ALTMO_CONFIG_FILE "
                "environment variable\n"
            )
            sys.exit(1)

        [setattr(self, key, val) for key, val in config_file_data.items()]

    def __getattribute__(self, attr: str):
        if attr.isupper():
            if not self._config_loaded:
                self._load_config()
                self._config_loaded = True
        return super().__getattribute__(attr)


_CONFIG = Config()


class TableNames:
    """
    Holds the table names and lazy loads the config object (it waits until a property is read)
    """
    STUDY_AREA_TBL: str = "study_areas"
    STUDY_PARTS_TBL: str = "study_area_parts"
    AMENITIES_TBL: str = "amenities"
    RESIDENCES_TBL: str = "residences"
    RES_AMENITY_DIST_TBL: str = "residence_amenity_distances"
    RES_AMENITY_DIST_STR_TBL: str = "residence_amenity_distances_straight"
    RES_AMENITY_CAT_DIST_TBL: str = "residence_amenity_category_distances"

    def __init__(self):
        self.config = None

    def __getattribute__(self, attr: str):
        if attr.isupper():
            if self.config is None:
                self.config = get_config_obj()
            return f'{self.config.TBL_PREFIX}{super().__getattribute__(attr)}'
        else:
            return super().__getattribute__(attr)


TABLES = TableNames()


def get_config_obj() -> Config:
    """
    Creates a config file that is intended to be loaded by each module using it.
    :return: Config object
    """
    config_file = get_config_file(DEFAULT_CONFIG_FILE_NAME)

    try:
        config_file_data = parse_config_file(config_file)
    except (FileNotFoundError, yaml.parser.ParserError):
        sys.stderr.write(
            f"Unable to parse {config_file} "
            "Please create this file and set ALTMO_CONFIG_FILE "
            "environment variable\n"
        )
        sys.exit(1)

    try:
        config = Config(**config_file_data)
    except TypeError:
        sys.stderr.write(
            "Unknown variables present in config file. Please see example for more information."
        )
        sys.exit(1)

    return config


def get_config_file(config_file_name) -> str:
    """
    Retrieves the filename used to store data.

    If the environment variable TRKR_DB_FILE is defined, we use this.
    If not, we use check the current directory before using the default
    location ($HOME_DIR/.altmo-config.yml)
    """
    if os.getenv("ALTMO_CONFIG_FILE"):
        return os.getenv("ALTMO_CONFIG_FILE")

    if os.path.exists(DEFAULT_CONFIG_FILE_NAME):
        return DEFAULT_CONFIG_FILE_NAME
    else:
        home_dir = os.path.expanduser("~")
        return os.path.join(home_dir, f".{config_file_name}")


def parse_config_file(config_file: str) -> dict:
    """parses the yml config file and returns its contents"""
    with open(config_file, "r") as file:
        return yaml.safe_load(file)


def get_config(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(_CONFIG, *args, **kwargs)
    return wrapper


def get_config_method(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        self, *_args = args
        return func(self, _CONFIG, *_args, **kwargs)
    return wrapper
