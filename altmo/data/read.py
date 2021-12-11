import json
from typing import List, Tuple


def get_study_area(cursor, name) -> tuple:
    """returns a single study area"""
    cursor.execute('SELECT id, name, description, geom FROM study_areas WHERE name = %s', (name,))

    return cursor.fetchone() or (None, None)


def get_residences(cursor, study_area_id: int, with_stats: bool = False) -> List[Tuple]:
    """get all the residences for a study area"""
    if with_stats:
        extra_join = '''
            JOIN residence_amenity_distance_standardized_categorized c
            ON r.id = c.residence_id
        '''
        extra_flds = '''
            , c.administrative_average_time, c.community_average_time, c.groceries_average_time,
            c.health_average_time, c.nature_average_time, c.outing_destination_average_time,
            c.school_average_time, c.shopping_average_time, c.all_average_time,
            c.mode
        '''
    else:
        extra_join = ''
        extra_flds = ''
    sql = f'''
    SELECT
        r.id, r.study_area_id, r.tags, r.house_number, r.building, r.geom {extra_flds}
    FROM
        residences r
    {extra_join}
    WHERE
        r.study_area_id = %s
    '''
    cursor.execute(sql, (study_area_id,))

    return cursor.fetchall()


def get_amenity_name_category(cursor, study_area_id: int, category=None, name=None) -> List[str]:
    """return all amenity names in amenity table for a single study_area"""
    sql = 'SELECT DISTINCT name, category FROM amenities WHERE study_area_id = %s'
    params = (study_area_id,)

    if category:
        sql += ' AND category = %s'
        params += (category,)

    if name:
        sql += ' AND name = %s'
        params += (name, )

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


def get_residence_amenity_straight_distance(
        cursor, residence_id: int, category: str = None, name: str = None) -> List[Tuple]:
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
    params = (residence_id, )

    if category is not None:
        sql += ' AND am.category = %s'
        params += (category, )

    if name is not None:
        sql += ' AND am.name = %s'
        params += (name, )

    cursor.execute(sql, params)

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


def get_residence_points_time_zscore_geojson(cursor, study_area_id: int, mode: str, srs_id: str = '4326') -> str:
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
                'geometry',   ST_AsGeoJSON(ST_Transform(geom, {srs_id}))::jsonb,
                'properties', to_jsonb(row) - 'id' - 'geom'
            ) AS feature
        FROM (
        SELECT
            r.id, r.geom,
            sc.all_time_zscore,
            sc.all_average_time,
            sc.administrative_time_zscore,
            sc.administrative_average_time,
            sc.community_time_zscore,
            sc.community_average_time,
            sc.groceries_time_zscore,
            sc.groceries_average_time,
            sc.health_time_zscore,
            sc.health_average_time,
            sc.nature_time_zscore,
            sc.nature_average_time,
            sc.outing_destination_time_zscore,
            sc.outing_destination_average_time,
            sc.school_time_zscore,
            sc.school_average_time,
            sc.shopping_time_zscore,
            sc.shopping_average_time
        FROM
            residences r
        JOIN
            residence_amenity_distance_standardized_categorized sc
        ON
            r.id = sc.residence_id
        WHERE
            r.study_area_id = %s
        AND
            sc.mode = %s
        ) row) features;
    '''

    cursor.execute(sql, (study_area_id, mode))

    result = cursor.fetchone()

    if result:
        return json.dumps(result[0])
    else:
        return json.dumps({})


def get_residence_points_all_web_geojson(cursor, study_area_id: int, mode: str) -> str:
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
                'properties', to_jsonb(row) - 'geom'
            ) AS feature
        FROM (
        SELECT
            r.id, r.geom,
            sc.all_average_time
        FROM
            residences r
        JOIN
            residence_amenity_distance_standardized_categorized sc
        ON
            r.id = sc.residence_id
        WHERE
            r.study_area_id = %s
        AND
            sc.mode = %s
        ) row) features;
    '''

    cursor.execute(sql, (study_area_id, mode))

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
