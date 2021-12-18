import json
import os
from unittest import mock

import pytest
from click.testing import CliRunner

from altmo.commands.export import export, EXPORT_TYPE_ALL, EXPORT_TYPE_SINGLE_RESIDENCE

from tests.fixtures.amenity import AMENITY_CATEGORY_PAIRS, get_residence_composite_average_times


@pytest.fixture()
def mock_register_hstore(mocker):
    """Mock the psycopg2.extras.register_hstore function"""
    mock_obj = mock.MagicMock()
    mocker.patch(
        'altmo.commands.export.register_hstore', mock_obj
    )

    return mock_obj


def test_type_all_happy_path(mock_db):
    """Test successful run of export"""
    # Set up mocks
    mock_con = mock_db.return_value
    mock_cur = mock_con.cursor.return_value
    mock_cur.fetchone.return_value = (1, 'new_york', 'New York study area')
    mock_cur.fetchall.side_effect = [
        AMENITY_CATEGORY_PAIRS,
        get_residence_composite_average_times()
    ]

    runner = CliRunner()
    result = runner.invoke(export, ['new_york', EXPORT_TYPE_ALL])

    assert result.exit_code == 0

    # make sure the output s valid GeoJSON
    out = json.loads(result.output)

    assert out['type'] == 'FeatureCollection'
    assert len(out['features']) == 10


def test_type_all_with_properties(mock_db):
    """Test successful run of export"""
    # Set up mocks
    mock_con = mock_db.return_value
    mock_cur = mock_con.cursor.return_value
    mock_cur.fetchone.return_value = (1, 'new_york', 'New York study area')
    mock_cur.fetchall.side_effect = [
        AMENITY_CATEGORY_PAIRS,
        get_residence_composite_average_times()
    ]

    runner = CliRunner()
    properties_str = 'nature,groceries,administrative'
    properties = sorted(properties_str.split(','))
    result = runner.invoke(export, ['new_york', EXPORT_TYPE_ALL, '--properties', properties_str])

    assert result.exit_code == 0

    # make sure the output s valid GeoJSON
    out = json.loads(result.output)

    assert out['type'] == 'FeatureCollection'
    assert len(out['features']) == 10
    assert sorted(list(out['features'][0]['properties'].keys())) == properties


def test_type_all_with_bad_properties(mock_db):
    """Test successful run of export"""
    # Set up mocks
    mock_con = mock_db.return_value
    mock_cur = mock_con.cursor.return_value
    mock_cur.fetchone.return_value = (1, 'new_york', 'New York study area')
    mock_cur.fetchall.side_effect = [
        AMENITY_CATEGORY_PAIRS,
        get_residence_composite_average_times()
    ]

    runner = CliRunner()
    properties_str = 'nature,groceries,administratiadfasfadfa'
    *_, bad_prop = properties_str.split(',')
    result = runner.invoke(export, ['new_york', EXPORT_TYPE_ALL, '--properties', properties_str])

    assert result.exit_code == 2
    assert f'"{bad_prop}" is not available. Choices are' in result.output


def test_type_single_residence_happy_path(mock_db, mock_register_hstore):
    """Test a run of the single_residence type without any errors"""
    # Set up mocks
    mock_con = mock_db.return_value
    mock_cur = mock_con.cursor.return_value
    mock_cur.fetchone.return_value = (1, 'new_york', 'New York study area')
    mock_cur.fetchall.side_effect = [
        AMENITY_CATEGORY_PAIRS,
        get_residence_composite_average_times()
    ]

    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            export, ['new_york', EXPORT_TYPE_SINGLE_RESIDENCE]
        )

        assert result.exit_code == 0
        assert result.output == ''

        exported_files = os.listdir('./export')

        assert len(exported_files) == 10


def test_type_single_residence_export_dir_exists_error(mock_db):
    """Test a run of the single_residence type without any errors"""
    # Set up mocks
    mock_con = mock_db.return_value
    mock_cur = mock_con.cursor.return_value
    mock_cur.fetchone.return_value = (1, 'new_york', 'New York study area')

    runner = CliRunner()
    with runner.isolated_filesystem():
        os.mkdir('export')

        result = runner.invoke(
            export, ['new_york', EXPORT_TYPE_SINGLE_RESIDENCE]
        )

        assert result.exit_code == 1
        assert result.output == 'export directory already exists\n'


def test_study_area_not_found(mock_db):
    """Test successful run of export"""
    # Set up mocks
    mock_con = mock_db.return_value
    mock_cur = mock_con.cursor.return_value
    mock_cur.fetchone.return_value = None

    runner = CliRunner()
    result = runner.invoke(export, ['new_york', EXPORT_TYPE_ALL])

    assert result.exit_code == 1
    assert result.output == 'study area not found\n'
