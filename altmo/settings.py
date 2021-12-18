import os
import sys
from dataclasses import dataclass

import yaml
import yaml.parser

# Static variables
DEFAULT_CONFIG_FILE_NAME = "altmo-config.yml"
MODE_PEDESTRIAN = "pedestrian"
MODE_BICYCLE = "bicycle"


@dataclass(frozen=True)
class Config:
    """Holds configuration which is loaded from config file"""

    TBL_PREFIX: str
    SRS_ID: int
    PG_DSN: str
    VALHALLA_SERVER: str
    AMENITIES: dict


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
    If not, we use the default location ($HOME_DIR/.altmo-config.yml)
    """
    if os.getenv("ALTMO_CONFIG_FILE"):
        return os.getenv("ALTMO_CONFIG_FILE")
    else:
        home_dir = os.path.expanduser("~")
        return os.path.join(home_dir, f".{config_file_name}")


def parse_config_file(config_file: str) -> dict:
    """parses the yml config file and returns its contents"""
    with open(config_file, "r") as file:
        return yaml.safe_load(file)
