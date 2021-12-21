import os
import sys
from dataclasses import dataclass
from functools import wraps
from contextlib import contextmanager

import yaml
import yaml.parser

# Static variables
DEFAULT_CONFIG_FILE_NAME = "altmo-config.yml"
MODE_PEDESTRIAN = "pedestrian"
MODE_BICYCLE = "bicycle"
PARALLEL_LOW = 'low'
PARALLEL_HIGH = 'high'


class ConfigPoolError(Exception):
    pass


@dataclass
class Config:
    """Holds configuration which is loaded from config file"""
    TBL_PREFIX: str = None
    SRS_ID: int = None
    PG_DSN: str = None
    VALHALLA_SERVER: str = None
    AMENITIES: dict = None

    def __post_init__(self):
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


class ConfigPool:
    def __init__(self, pool_size: int = 1) -> None:
        """Initialize ConfigPool with the size of the pool"""
        self.pool_size: int = pool_size
        self.pool: list[Config] = []
        self._loaded = False

    def acquire(self) -> Config:
        """
        Acquires a Config object from pool

        We wait until the first time acquire is called to populate the pool.
        """
        if not self._loaded:
            self.pool = [Config() for _ in range(self.pool_size)]
        if len(self.pool) <= 0:
            raise ConfigPoolError('ConfigPool exhausted. Release ')
        return self.pool.pop()

    def release(self, config: Config):
        """Releases Config object back into our pool"""
        self.pool.append(config)


_config_pool = ConfigPool(pool_size=1)


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
        self._tbl_prefix = None

    def __getattribute__(self, attr: str):
        """
        Overrides default getattr behavior, so we can load the TBL_PREFIX
        value from our config object. We cache this value for later use.
        """
        if attr.isupper():
            if self._tbl_prefix is None:
                with config_manager() as config:
                    self._tbl_prefix = config.TBL_PREFIX
            return f'{self._tbl_prefix}{super().__getattribute__(attr)}'
        else:
            return super().__getattribute__(attr)


TABLES = TableNames()


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


def get_config(func):
    """Decorator function for acquiring and releasing a config object"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        config = _config_pool.acquire()
        ret_val = func(config, *args, **kwargs)
        _config_pool.release(config)
        return ret_val
    return wrapper


@contextmanager
def config_manager():
    """Context manager for acquiring and releasing a config object"""
    config = _config_pool.acquire()
    yield config
    _config_pool.release(config)
