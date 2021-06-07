from typing import List, Tuple


def get_study_area(cursor, name) -> tuple:
    """returns a single study area"""
    cursor.execute('SELECT id, name, description, geom FROM study_areas WHERE name = %s', (name, ))

    return cursor.fetchone() or (None, None)


def get_residences(cursor, study_area_id: int) -> List[Tuple]:
    """get all the residences for a study area"""
    sql = '''
    SELECT
        id, study_area_id, tags, house_number, building, geom
    FROM
        residences
    WHERE
        "house_number" IS NOT NULL
    AND
        building IN ('yes', 'house', 'residential', 'apartments')
    AND
        study_area_id = %s
    '''
    cursor.execute(sql, (study_area_id, ))

    return cursor.fetchall()


def get_amenity_names(cursor, study_area_id: int) -> List[str]:
    """return all amenity names in amenity table for a single study_area"""
    cursor.execute('SELECT DISTINCT name FROM amenities WHERE study_area_id = %s', (study_area_id, ))
    return [x for x, *_ in cursor.fetchall()]
