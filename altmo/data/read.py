import json
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


def get_study_area_parts_all_geojson(cursor, study_area_id: int) -> str:
    sql = '''
    SELECT jsonb_build_object(
        'type',     'FeatureCollection',
        'features', jsonb_agg(feature)
    )
    FROM (
        SELECT
            json_build_object(
                'type', 'Feature',
                'id', id,
                'geometry', ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb,
                'properties', to_jsonb(row) - 'id' - 'geom'
            ) AS feature
        FROM (
        SELECT
            st.id, st.geom, st.name, st.description, 
            AVG(sc.all_time_zscore) as all_time_zscore,
            AVG(sc.all_distance_zscore) as all_distance_zscore,
            AVG(sc.administrative_time_zscore) as administrative_time_zscore,
            AVG(sc.administrative_distance_zscore) as administrative_distance_zscore,
            AVG(sc.community_time_zscore) as community_time_zscore,
            AVG(sc.community_distance_zscore) as community_distance_zscore,
            AVG(sc.groceries_time_zscore) as groceries_time_zscore,
            AVG(sc.groceries_distance_zscore) as groceries_distance_zscore,
            AVG(sc.health_time_zscore) as health_time_zscore,
            AVG(sc.health_distance_zscore) as health_distance_zscore,
            AVG(sc.nature_time_zscore) as nature_time_zscore,
            AVG(sc.nature_distance_zscore) as nature_distance_zscore,
            AVG(sc.outing_destination_time_zscore) as outing_destination_time_zscore,
            AVG(sc.outing_destination_distance_zscore) as outing_destination_distance_zscore,
            AVG(sc.school_time_zscore) as school_time_zscore,
            AVG(sc.school_distance_zscore) as school_distance_zscore,
            AVG(sc.shopping_time_zscore) as shopping_time_zscore,
            AVG(sc.shopping_distance_zscore) as shopping_distance_zscore
        FROM
            study_area_parts st
        JOIN
            residences r
        ON
            ST_Contains(st.geom, r.geom)
        JOIN
            residence_amenity_distance_standardized_categorized sc
        ON
            r.id = sc.residence_id
        WHERE
            st.study_area_id = %s AND r.study_area_id = %s
        GROUP BY
            st.id, st.geom, st.name, st.description
        ) row) features;
    '''

    cursor.execute(sql, (study_area_id, study_area_id))

    result = cursor.fetchone()

    if result:
        return json.dumps(result[0])
    else:
        return json.dumps({})


def get_residence_points_time_zscore_geojson(cursor, study_area_id: int) -> str:
    sql = '''
    SELECT jsonb_build_object(
        'type',     'FeatureCollection',
        'features', jsonb_agg(feature)
    )
    FROM (
        SELECT
            json_build_object(
                'type',       'Feature',
                'id',         id,
                'geometry',   ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb,
                'properties', to_jsonb(row) - 'id' - 'geom'
            ) AS feature
        FROM (
        SELECT
            r.id, r.geom,
            AVG(sc.all_time_zscore) as all_time_zscore,
            AVG(sc.all_distance_zscore) as all_distance_zscore
            -- AVG(sc.administrative_time_zscore) as administrative_time_zscore,
            -- AVG(sc.administrative_distance_zscore) as administrative_distance_zscore,
            -- AVG(sc.community_time_zscore) as community_time_zscore,
            -- AVG(sc.community_distance_zscore) as community_distance_zscore,
            -- AVG(sc.groceries_time_zscore) as groceries_time_zscore,
            -- AVG(sc.groceries_distance_zscore) as groceries_distance_zscore,
            -- AVG(sc.health_time_zscore) as health_time_zscore,
            -- AVG(sc.health_distance_zscore) as health_distance_zscore,
            -- AVG(sc.nature_time_zscore) as nature_time_zscore,
            -- AVG(sc.nature_distance_zscore) as nature_distance_zscore,
            -- AVG(sc.outing_destination_time_zscore) as outing_destination_time_zscore,
            -- AVG(sc.outing_destination_distance_zscore) as outing_destination_distance_zscore,
            -- AVG(sc.school_time_zscore) as school_time_zscore,
            -- AVG(sc.school_distance_zscore) as school_distance_zscore,
            -- AVG(sc.shopping_time_zscore) as shopping_time_zscore,
            -- AVG(sc.shopping_distance_zscore) as shopping_distance_zscore
        FROM
            residences r
        JOIN
            residence_amenity_distance_standardized_categorized sc
        ON
            r.id = sc.residence_id
        WHERE
            r.study_area_id = %s
        GROUP BY
            r.id, r.geom
        ) row) features;
    '''

    cursor.execute(sql, (study_area_id, ))

    result = cursor.fetchone()

    if result:
        return json.dumps(result[0])
    else:
        return json.dumps({})


def get_residence_points_time_geojson(cursor, study_area_id: int, category: str = 'all') -> str:
    if category == 'all':
        where_sql = ''
    else:
        where_sql = 'AND s.amenity_category = %s'
    sql = f'''
    SELECT jsonb_build_object(
        'type',     'FeatureCollection',
        'features', jsonb_agg(feature)
    )
    FROM (
        SELECT
            json_build_object(
                'type',       'Feature',
                'id',         id,
                'geometry',   ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb,
                'properties', to_jsonb(row) - 'id' - 'geom'
            ) AS feature
        FROM (
        SELECT
            r.id, r.geom,
            FLOOR(AVG(s.average_time) / 60.0) as minutes,
            AVG(s.average_time)::integer %% 60 as seconds
        FROM
            residence_amenity_distance_standardized s
        JOIN
            residences r
        ON
            r.id = s.residence_id
        WHERE
            r.study_area_id = %s
        {where_sql}
        GROUP BY
            r.id, r.geom
        ) row) features;
    '''
    if category == 'all':
        cursor.execute(sql, (study_area_id, ))
    else:
        cursor.execute(sql, (study_area_id, category))

    result = cursor.fetchone()

    if result:
        return json.dumps(result[0])
    else:
        return json.dumps({})
