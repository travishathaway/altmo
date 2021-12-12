import os
import sys

import yaml
import yaml.parser

TBL_PREFIX = os.getenv('ALTMO_TBL_PREFIX', 'altmo_')
SRS_ID = os.getenv('ALTMO_SRS_ID', 3857)
CONFIG_FILE_NAME = 'altmo-config.yml'


def get_config_file(config_file_name) -> str:
    """
    Retrieves the filename used to store data.

    If the environment variable TRKR_DB_FILE is defined, we use this.
    If not, we use the default location ($HOME_DIR/.altmo-config.yml)
    """
    if os.getenv('ALTMO_CONFIG_FILE'):
        return os.getenv('ALTMO_CONFIG_FILE')
    else:
        home_dir = os.path.expanduser('~')
        return os.path.join(home_dir, f'.{config_file_name}')


def parse_config_file(config_file: str) -> dict:
    """parses the yml config file and returns its contents"""
    with open(config_file, 'r') as file:
        return yaml.safe_load(file)


try:
    CONFIG_DATA = parse_config_file(get_config_file(CONFIG_FILE_NAME))
    PG_DSN = CONFIG_DATA['pg_dsn']
    VALHALLA_SERVER = CONFIG_DATA['valhalla_server']

except (IOError, yaml.parser.ParserError):
    sys.stderr.write(
        'Unable to parse altmo-config.yml. '
        'Please create this file and set ALTMO_CONFIG_FILE '
        'environment variable\n'
    )
    sys.exit(1)
