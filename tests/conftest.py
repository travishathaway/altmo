import os
import tempfile
from unittest import mock
import random

import yaml
import pytest

from tests.fixtures.config_data import CONFIG_DATA

random.seed(1)


@pytest.fixture(autouse=True, scope='session')
def config_file():
    """
    - Writes a temporary config file to disk
    - Sets its location as an environment variable
    - Adds default config
    """
    _, tmp_path = tempfile.mkstemp(suffix='config.yml')
    with open(tmp_path, 'w') as fp:
        fp.write(yaml.dump(CONFIG_DATA))

    os.environ['ALTMO_CONFIG_FILE'] = tmp_path

    yield tmp_path

    # remove file
    os.unlink(tmp_path)


@pytest.fixture()
def mock_db(mocker) -> mock.MagicMock:
    """
    Replaces our cursor with a mock object
    """
    mock_connect = mock.MagicMock()

    mocker.patch(
        'altmo.data.decorators.psycopg2.connect', mock_connect
    )

    return mock_connect


@pytest.fixture()
def mock_cur_study_area(mock_db):
    """
    Sets up a study area as a mock on our mock_db fixture
    """
    mock_con = mock_db.return_value
    mock_cur = mock_con.cursor.return_value
    mock_cur.fetchone.return_value = (1, 'new_york', 'New York study area')

    return mock_cur
