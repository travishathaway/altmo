import json

from click.testing import CliRunner
from psycopg2.errors import UniqueViolation

from altmo.commands.create_study_area import create_study_area

from tests.fixtures.boundary import BOUNDARY_NEW_YORK


def test_happy_path(mock_db):
    """Test a successful run of this command"""
    runner = CliRunner()
    boundary_file = 'boundary.geojson'
    srs_id = BOUNDARY_NEW_YORK['crs']['properties']['srs_id']
    with runner.isolated_filesystem():
        with open(boundary_file, 'w') as fp:
            fp.write(json.dumps(BOUNDARY_NEW_YORK))

        result = runner.invoke(
            create_study_area, [boundary_file, 'new_york', 'New York study area', srs_id]
        )

        assert result.exit_code == 0
        assert result.output == ''


def test_bad_geojson(mock_db):
    """Test the case where a bad geojson file is passed in"""
    runner = CliRunner()
    boundary_file = 'boundary.geojson'
    srs_id = BOUNDARY_NEW_YORK['crs']['properties']['srs_id']
    with runner.isolated_filesystem():
        with open(boundary_file, 'w') as fp:
            fp.write('this is not a geojson file')

        result = runner.invoke(
            create_study_area, [boundary_file, 'new_york', 'New York study area', srs_id]
        )

        assert result.exit_code == 1
        assert result.output == 'Malformed GeoJSON\n'


def test_study_area_already_exists_error(mock_db):
    """Test the case where record is already present in database"""
    # Set up mocks
    mock_con = mock_db.return_value
    mock_cur = mock_con.cursor.return_value
    mock_cur.execute.side_effect = UniqueViolation('This value is already in the database!')

    runner = CliRunner()
    boundary_file = 'boundary.geojson'
    srs_id = BOUNDARY_NEW_YORK['crs']['properties']['srs_id']
    with runner.isolated_filesystem():
        with open(boundary_file, 'w') as fp:
            fp.write(json.dumps(BOUNDARY_NEW_YORK))

        result = runner.invoke(
            create_study_area, [boundary_file, 'new_york', 'New York study area', srs_id]
        )

        assert result.exit_code == 1
        assert result.output == 'Name already exists in database\n'
