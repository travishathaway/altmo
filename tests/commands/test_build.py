from click.testing import CliRunner

from altmo.commands.build import build


def test_happy_path(mock_db):
    """Test simple invocation"""
    # Set up mocks
    mock_con = mock_db.return_value
    mock_cur = mock_con.cursor.return_value
    mock_cur.connection.encoding = 'UTF8'  # This is here because we call extra_values in the psycopg2.extras module
    mock_cur.fetchone.return_value = (1, 'new_york', 'New York study area')

    runner = CliRunner()
    result = runner.invoke(build, ['new_york'])

    assert result.exit_code == 0
    assert result.output == ''


def test_study_area_not_found(mock_db):
    """Test simple invocation"""
    # Set up mocks
    mock_con = mock_db.return_value
    mock_cur = mock_con.cursor.return_value
    mock_cur.fetchone.return_value = None

    runner = CliRunner()
    result = runner.invoke(build, ['new_york'])

    assert result.exit_code == 0
    assert result.output == 'study area not found\n'
