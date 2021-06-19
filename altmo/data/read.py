from typing import List, Tuple


def get_study_area(cursor, name) -> tuple:
    """returns a single study area"""
    cursor.execute('SELECT id, name, description, geom FROM study_areas WHERE name = %s', (name,))

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
    cursor.execute(sql, (study_area_id,))

    return cursor.fetchall()


def get_amenity_name_category(cursor, study_area_id: int, category=None) -> List[str]:
    """return all amenity names in amenity table for a single study_area"""
    sql = 'SELECT DISTINCT name, category FROM amenities WHERE study_area_id = %s'
    params = (study_area_id,)

    if category:
        sql += ' AND category = %s'
        params += (category,)

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
    cursor.execute(sql, (study_area_id,))
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

    cursor.execute(sql, (residence_id,))

    return cursor.fetchall()


def get_category_time_zscores(cursor, study_area_id: int) -> List[Tuple]:
    sql = '''
    SELECT
        d.residence_id,
        SUM(
            CASE
                WHEN d.amenity_category = 'administrative'
                THEN
                    CASE
                        WHEN d.amenity_name = 'bank' OR d.amenity_name = 'post_box' OR d.amenity_name = 'town_hall'
                        THEN d.time_zscore * 0.25
                        WHEN d.amenity_name = 'police' OR d.amenity_name = 'post_office'
                        THEN d.time_zscore * 0.125
                    END
            END
        ) as administrative_time_zscore,
            SUM(
            CASE
                WHEN d.amenity_category = 'community'
                THEN
                    CASE
                        WHEN d.amenity_name = 'community_centre' OR d.amenity_name = 'social_facility'
                        THEN d.time_zscore * 0.333
                        WHEN d.amenity_name = 'library'
                        THEN d.time_zscore * 0.334
                    END
            END
        ) as community_time_zscore,
            SUM(
            CASE
                WHEN d.amenity_category = 'groceries'
                THEN
                    CASE
                        WHEN d.amenity_name = 'bakery' OR d.amenity_name = 'butcher'
                        THEN d.time_zscore * 0.25
                        WHEN d.amenity_name = 'supermarket'
                        THEN d.time_zscore * 0.5
                    END
            END
        ) as groceries_time_zscore,
            SUM(
            CASE
                WHEN d.amenity_category = 'health'
                THEN
                    CASE
                        WHEN d.amenity_name = 'clinic' OR d.amenity_name = 'nursing_home' 
                            OR d.amenity_name = 'veterinary'
                        THEN d.time_zscore * 0.1
                        WHEN d.amenity_name = 'dentist' OR d.amenity_name = 'hospital'
                        THEN d.time_zscore * 0.15
                        WHEN d.amenity_name = 'doctors' OR d.amenity_name = 'pharmacy'
                        THEN d.time_zscore * 0.2
                    END
            END
        ) as health_time_zscore,
        SUM(
            CASE
                WHEN d.amenity_category = 'nature'
                THEN
                    CASE
                        WHEN d.amenity_name = 'allotment' OR d.amenity_name = 'cemetery'
                        THEN d.time_zscore * 0.1
                        WHEN d.amenity_name = 'forest'
                        THEN d.time_zscore * 0.2
                        WHEN d.amenity_name = 'park' OR d.amenity_name = 'sports'
                        THEN d.time_zscore * 0.3
                    END
            END
        ) as nature_time_zscore,
        SUM(
            CASE
                WHEN d.amenity_category = 'outing_destination'
                THEN d.time_zscore * 0.1
            END
        ) as outing_destination_zscore,
        SUM(
            CASE
                WHEN d.amenity_category = 'school'
                THEN
                    CASE
                        WHEN d.amenity_name = 'driving_school' OR d.amenity_name = 'music_school' 
                            OR d.amenity_name = 'research_institute'
                        THEN d.time_zscore * 0.05
                        WHEN d.amenity_name = 'college'
                        THEN d.time_zscore * 0.125
                        WHEN d.amenity_name = 'kindergarten'
                        THEN d.time_zscore * 0.15
                        WHEN d.amenity_name = 'university'
                        THEN d.time_zscore * 0.175
                        WHEN d.amenity_name = 'school' OR d.amenity_name = 'childcare'
                        THEN d.time_zscore * 0.2
                    END
            END
        ) as school_time_zscore,
        SUM(
            CASE
                WHEN d.amenity_category = 'shopping'
                THEN
                    CASE
                        WHEN d.amenity_name = 'clothes'
                        THEN d.time_zscore * 0.112
                        ELSE d.time_zscore * 0.111
                    END
            END
        ) as shopping_time_zscore
    FROM
        residence_amenity_distance_standardized d
    JOIN
        residences r
    ON
        r.id = d.residence_id
    WHERE
        r.study_area_id = %s
    GROUP BY
        d.residence_id
    '''

    cursor.execute(cursor, (study_area_id, ))

    return cursor.fetchall()
