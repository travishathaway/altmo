import os
from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner

from altmo.commands.network_distances import network_distances
from altmo.validators import OUT_STDOUT, OUT_CSV

from tests.fixtures.valhalla import VALHALLA_MATRIX_RESPONSE
from tests.fixtures.straight_distance import STRAIGHT_DISTANCE


@pytest.fixture()
def mock_cur_straight_dist(mock_cur_study_area):
    """
    Sets up our mock cursor with a couple of return values we will need for multiple tests.
    """
    mock_cur_study_area.fetchall.return_value = STRAIGHT_DISTANCE

    return mock_cur_study_area


def test_happy_path_out_csv(mock_cur_straight_dist):
    """
    Happy path setting --out to "stdout"
    """
    with patch('altmo.api.valhalla.aiohttp.ClientSession.post', AsyncMock()) as mock_post:
        valhalla_mock_resp = AsyncMock(return_value=VALHALLA_MATRIX_RESPONSE)
        mock_post.return_value.json = valhalla_mock_resp

        runner = CliRunner()

        # with runner.isolated_filesystem():
        filename = 'test-out.csv'
        result = runner.invoke(network_distances, ['new_york', '--out', OUT_CSV, '--file-name', filename])

        assert result.exit_code == 0
        assert result.output == ''

        # cur_dir = os.listdir('./')
        # for filename in cur_dir:
        #     with open(filename) as fp:
        #         assert len(list(fp)) == 10


def test_happy_path_out_stdout(mock_cur_straight_dist):
    """
    Happy path setting --out to "csv"
    """
    with patch('altmo.api.valhalla.aiohttp.ClientSession.post', AsyncMock()) as mock_post:
        valhalla_mock_resp = AsyncMock(return_value=VALHALLA_MATRIX_RESPONSE)
        # Mocks the pool async context manager and the session context manager
        mock_post.return_value.json = valhalla_mock_resp

        runner = CliRunner()
        result = runner.invoke(network_distances, ['new_york', '--out', OUT_STDOUT])

        assert result.exit_code == 0
        assert result.output != ''
