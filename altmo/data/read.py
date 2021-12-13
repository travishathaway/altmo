import json
from typing import List, Tuple, Dict

from altmo.utils import get_category_amenity_keys

from .schema import (
    STUDY_AREA_TBL,
    STUDY_PARTS_TBL,
    RESIDENCES_TBL,
    AMENITIES_TBL,
    RES_AMENITY_DIST_TBL,
    RES_AMENITY_DIST_STR_TBL,
    RES_AMENITY_CAT_DIST_TBL,
    RES_AMENITY_STD_CAT_TBL
)


def get_study_area(cursor, name) -> tuple:
    """returns a single study area"""
    cursor.execute(f'SELECT id, name, description, geom FROM {STUDY_AREA_TBL} WHERE name = %s', (name,))

    return cursor.fetchone() or (None, None)


def get_residences(cursor, study_area_id: int, with_stats: bool = False) -> List[Tuple]:
    """get all the residences for a study area"""
    if with_stats:
        extra_join = f'''
            JOIN {RES_AMENITY_STD_CAT_TBL} c
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
        {RESIDENCES_TBL} r
    {extra_join}
    WHERE
        r.study_area_id = %s
    '''
    cursor.execute(sql, (study_area_id,))

    return cursor.fetchall()


def get_amenity_name_category(cursor, study_area_id: int, category=None, name=None) -> List[tuple]:
    """return all amenity names in amenity table for a single study_area"""
    sql = f'SELECT DISTINCT name, category FROM {AMENITIES_TBL} WHERE study_area_id = %s'
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
    sql = f'''
        SELECT 
            count(*) 
        FROM 
            {RES_AMENITY_DIST_STR_TBL} ra
        JOIN
            {RESIDENCES_TBL} r
        ON
            ra.residence_id = r.id
        JOIN
            {AMENITIES_TBL} a
        ON
            ra.amenity_id = a.id
        WHERE a.study_area_id = %s AND r.study_area_id = %s
    '''
    cursor.execute(sql, (study_area_id, study_area_id))
    return cursor.fetchone()[0]


def get_study_area_residences(cursor, study_area_id: int) -> List[Tuple]:
    """fetch all residences for a study area"""
    sql = f'''
        SELECT 
            id, ST_X(ST_Transform(geom, 4326)), ST_Y(ST_Transform(geom, 4326))
        FROM 
            {RESIDENCES_TBL}
        WHERE study_area_id = %s
    '''
    cursor.execute(sql, (study_area_id,))
    return cursor.fetchall()


def get_residence_amenity_straight_distance(
        cursor, residence_id: int, category: str = None, name: str = None) -> List[Tuple]:
    sql = f'''
    SELECT 
        amenity_id, ST_X(ST_Transform(am.geom, 4326)), ST_Y(ST_Transform(am.geom, 4326))
    FROM 
        {RES_AMENITY_DIST_STR_TBL}
    JOIN
        {AMENITIES_TBL} am
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
    sql = f'''
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
            {STUDY_PARTS_TBL} st
        JOIN
            {RESIDENCES_TBL} r
        ON
            ST_Contains(st.geom, r.geom)
        JOIN
            {RES_AMENITY_STD_CAT_TBL} sc
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
            {RESIDENCES_TBL} r
        JOIN
            {RES_AMENITY_STD_CAT_TBL} sc
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
                'properties', to_jsonb(row) - 'geom'
            ) AS feature
        FROM (
        SELECT
            r.id, r.geom,
            sc.all_average_time
        FROM
            {RESIDENCES_TBL} r
        JOIN
            {RES_AMENITY_STD_CAT_TBL} sc
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
            {RES_AMENITY_CAT_DIST_TBL} s
        JOIN
            {RESIDENCES_TBL} r
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


def get_residence_composite_average_times(cursor, study_area_id: int, mode: str, weights: Dict[str, Dict]) -> List:
    """
    Retrieves a list of residences with their composite averages based on amenities config.

    This function:
        - builds a rather complex SQL query (using the `crosstab` function)
        - executes this query
        - returns the results

    Admittedly it is a bit messy. One thing to watch out for is the `sub_sql` string
    which has to be escaped because it is being passed to the `crosstab` function.

    SQL INJECTION VULNERABLE!!! (Never use this function with untrusted inputs!!!)

    :param cursor: psycopg2.cursor used for executing queries
    :param study_area_id: Used to narrow our query to study area we are interested in
    :param mode: Used to limit the results to a mode a transport ('pedestrian' or 'bicycle')
    :param weights: Mapping used to pass in weighting information (important for the composite index), but
                    also lets the function know which category amenity pairs to retrieve
    """
    amenities = get_amenity_name_category(cursor, study_area_id)
    config_cat_amts = get_category_amenity_keys(weights)

    # if it is not in our database or the config file, we do not use it
    amenity_categories = sorted([
        f'{category}_{amenity}' for amenity, category in amenities
        if f'{category}_{amenity}' in config_cat_amts
    ])
    cat_amt_filter_str = "'', ''".join(amenity_categories)

    sub_sql = f'''
    SELECT
        ra.residence_id as id, a.category || ''_'' || a.name as category, avg(ra.time) as avg_time
    FROM
        {RES_AMENITY_DIST_TBL} ra
    LEFT JOIN
        {AMENITIES_TBL} a
    ON
        a.id = ra.amenity_id
    WHERE
        ra.mode = ''{mode}''
    AND
        a.study_area_id = {study_area_id}
    AND
        a.category || ''_'' || a.name IN (''{cat_amt_filter_str}'')
    GROUP BY
        ra.residence_id, a.category, a.name, ra.mode
    ORDER BY
        ra.residence_id
    '''

    distinct_sql = f'''
    SELECT
        DISTINCT a.category || ''_'' || a.name AS category
    FROM
        {RES_AMENITY_DIST_TBL} ra
    LEFT JOIN
        {AMENITIES_TBL} a
    ON 
        a.id = ra.amenity_id
    WHERE
        a.category || ''_'' || a.name IN (''{cat_amt_filter_str}'')
    ORDER BY 
        a.category || ''_'' || a.name
    '''

    # Define the columns we extract from the `crosstab` function
    pivot_columns_with_type = [f'{col} NUMERIC' for col in amenity_categories]
    pivot_columns_str = ', '.join(pivot_columns_with_type)

    def get_category_avg_stmts_str() -> str:
        cat_avg_stmts: List[str] = []

        for category, amts in weights.items():
            wght_stmts = []
            for amenity, wght in amts.items():
                if (amenity, category) in [(a, c) for a, c in amenities]:
                    wght_stmts.append(f'{category}_{amenity} * {wght["weight"]}')
            wght_str = '+ '.join(wght_stmts)
            cat_avg_stmts.append(f'({wght_str}) as {category}')

        return ','.join(cat_avg_stmts)

    # Used for the top level select in our query
    categories = {c for _, c in amenities if c in weights}
    categories_str = ', '.join(categories)

    all_avg_str = ''
    if len(amenities) > 0:
        factor = 1.0 / len(categories)
        all_avg_str = '+ '.join([f'{c} * {factor}' for c in categories])
        all_avg_str = f' ({all_avg_str}) as all'

    sql = f'''
    SELECT 
        residence_id, {all_avg_str}, {categories_str}
    FROM (
        SELECT
            residence_id, {get_category_avg_stmts_str()}
        FROM 
            crosstab('{sub_sql}', '{distinct_sql}')
        AS (
            residence_id INTEGER, {pivot_columns_str}
        )
    ) as sub
    '''

    cursor.execute(sql)

    return cursor.fetchall()
