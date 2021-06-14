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


def get_amenity_name_category(cursor, study_area_id: int, category=None) -> List[str]:
    """return all amenity names in amenity table for a single study_area"""
    sql = 'SELECT DISTINCT name, category FROM amenities WHERE study_area_id = %s'
    params = (study_area_id, )

    if category:
        sql += ' AND category = %s'
        params += (category, )

    cursor.execute(sql, params)
    return cursor.fetchall()


def get_residence_amenity_straight_distance_count(cursor, study_area_id: int) -> int:
    sql = '''
        SELECT 
            count(*) 
        FROM 
            residence_amenity_distances_straight ra
        JOIN
            residences r
        ON
            ra.residence_id = r.id
        JOIN
            amenities a
        ON
            ra.amenity_id = a.id
        WHERE a.study_area_id = %s AND r.study_area_id = %s
    '''
    cursor.execute(sql, (study_area_id, study_area_id))
    return cursor.fetchone()[0]


def get_study_area_residences(cursor, study_area_id: int) -> List[Tuple]:
    """fetch all residences for a study area"""
    sql = '''
        SELECT 
            id, ST_X(ST_Transform(geom, 4326)), ST_Y(ST_Transform(geom, 4326))
        FROM 
            residences WHERE study_area_id = %s
    '''
    cursor.execute(sql, (study_area_id, ))
    return cursor.fetchall()


def get_residence_amenity_straight_distance(cursor, residence_id: int) -> List[Tuple]:
    sql = '''
    SELECT 
        amenity_id, ST_X(ST_Transform(am.geom, 4326)), ST_Y(ST_Transform(am.geom, 4326))
    FROM 
        residence_amenity_distances_straight
    JOIN
        amenities am
    ON
        amenity_id = am.id
    WHERE
        residence_id = %s
    '''

    cursor.execute(sql, (residence_id, ))

    return cursor.fetchall()
