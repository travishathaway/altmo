import json
from typing import List, Tuple, Union, Dict, Generator

from psycopg2.extras import execute_values

from .read import get_amenity_name_category
from .schema import (
    STUDY_AREA_TBL, AMENITIES_TBL, RESIDENCES_TBL,
    RES_AMENITY_DIST_TBL, RES_AMENITY_DIST_STR_TBL,
    RES_AMENITY_CAT_DIST_TBL
)


def create_study_area(cursor, data: dict, srs_id: Union[int, str]) -> None:
    """
    Creates a new study area.

    :raises: IndexError, KeyError, psycopg2.errors.UniqueViolation
    """
    sql = f'''
        INSERT INTO {STUDY_AREA_TBL} (name, description, geom)
        VALUES (%s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), {srs_id}))
    '''
    name = data['name']
    description = data['description']
    [feature] = data['features']
    geometry = feature['geometry']

    cursor.execute(sql, (name, description, json.dumps(geometry)))


def delete_amenities(cursor, study_area_id: int) -> None:
    """removes all amenities for a study area"""
    sql = f'DELETE FROM {AMENITIES_TBL} WHERE study_area_id = %s'
    cursor.execute(sql, (study_area_id, ))


def add_amenities(cursor, study_area_id: int, amenities: List[str]) -> None:
    """adds amenities to the amenities table without category"""
    amenities_sql_str = "','".join(amenities)

    # Add points from amenities column
    sql = f'''
    INSERT INTO {AMENITIES_TBL} (name, geom, study_area_id)
    SELECT 
        amenity, way, {study_area_id}
    FROM 
        planet_osm_point pp
    WHERE 
        ST_Contains((SELECT geom FROM {STUDY_AREA_TBL} where id = %s), pp.way)
    AND
        amenity IN ('{amenities_sql_str}')
    '''
    cursor.execute(sql, (study_area_id, ))

    # Add polygons from amenities column
    sql = f'''
    INSERT INTO {AMENITIES_TBL} (name, geom, study_area_id)
    SELECT 
        amenity, ST_Centroid(way), {study_area_id}
    FROM 
        planet_osm_polygon pp
    WHERE 
        ST_Contains((SELECT geom FROM {STUDY_AREA_TBL} WHERE id = %s), pp.way)
    AND
        amenity IN ('{amenities_sql_str}')
    '''
    cursor.execute(sql, (study_area_id, ))

    # Add points from shop column
    sql = f'''
    INSERT INTO {AMENITIES_TBL} (name, geom, study_area_id)
    SELECT 
        shop, way, {study_area_id}
    FROM 
        planet_osm_point pp
    WHERE 
        ST_Contains((SELECT geom FROM {STUDY_AREA_TBL} WHERE id = %s), pp.way)
    AND
        shop IN ('{amenities_sql_str}')
    '''
    cursor.execute(sql, (study_area_id, ))

    # Add polygons from shop column
    sql = f'''
    INSERT INTO {AMENITIES_TBL} (name, geom, study_area_id)
    SELECT 
        shop, ST_Centroid(way), {study_area_id}
    FROM 
        planet_osm_polygon pp
    WHERE 
        ST_Contains((SELECT geom FROM {STUDY_AREA_TBL} WHERE id = %s), pp.way)
    AND
        shop IN ('{amenities_sql_str}')
    '''
    cursor.execute(sql, (study_area_id, ))


def add_natural_amenities(cursor, study_area_id: int) -> None:
    """
    Runs special queries that add natural amenities for a study area.

    TODO: Refactor

    This function runs a complicated and very locale specific query
    for determining natural areas in a study area. This needs to be broken
    up a little (function for adding parks, playgrounds, etc.)

    Another problem here is that we assume an SRS that uses meters.
    """
    sql = f'''
    SELECT
        (ST_Dump(ST_GeneratePoints(pp.way, (ceil(pp.way_area/50000.0))::integer))).geom, 
        landuse, leisure, "natural"
    FROM
        planet_osm_polygon pp
    WHERE
        ST_Contains((SELECT geom FROM {STUDY_AREA_TBL} WHERE id = %s), pp.way)
    AND
        ("access" is null OR "access" = 'yes')
    AND (
        "landuse" in (
            'cemetery', 'recreation_ground', 'greenfield', 'allotments'
        )
        OR
        "leisure" in ('park', 'playground')
        OR
        ("leisure" = 'pitch' AND "sport" is not null)
        OR
        (name like '%%Gehölz%%' and ("natural" = 'wood' or "landuse" = 'forest'))
        OR
        name like '%%Gehege%%' 
        OR
        ("landuse" = 'forest' AND "way_area" > 50000)
    )
    '''
    cursor.execute(sql, (study_area_id, ))

    records = []

    # group the rows into "nature" categories
    for geom, landuse, leisure, natural in cursor.fetchall():
        if landuse == 'allotments':
            records.append((geom, 'nature', 'allotment', study_area_id))
        elif landuse == 'cemetery':
            records.append((geom, 'nature', 'cemetery', study_area_id))
        elif leisure == 'pitch' or landuse == 'recreation_ground':
            records.append((geom, 'nature', 'sports', study_area_id))
        elif landuse == 'forest' or natural == 'wood' or landuse == 'greenfield':
            records.append((geom, 'nature', 'forest', study_area_id))
        elif leisure == 'park' or leisure == 'playground':
            records.append((geom, 'nature', 'park', study_area_id))

    insert_sql = f'''
        INSERT INTO {AMENITIES_TBL} (geom, category, name, study_area_id) VALUES %s
    '''
    execute_values(
        cursor, insert_sql, records, template=None, page_size=100
    )


def add_amenities_category(cursor, study_area_id: int, amenity_category_map: Dict[str, str]) -> None:
    values = amenity_category_map.items()
    values_str = ','.join([
        f"('{name}', '{category}')"
        for name, category in values
    ])

    sql = f'''
    UPDATE {AMENITIES_TBL} as a set
        category = c.category
    FROM (VALUES
        {values_str}
    ) AS 
        c(name, category) 
    WHERE 
        c.name = a.name
    AND
        study_area_id = %s
    '''

    cursor.execute(sql, (study_area_id, ))


def add_residences(cursor, study_area_id: int) -> None:
    """copy residences from the OSM tables to our custom table"""
    sql = f'''
    WITH boundary AS (
      SELECT ST_Union(way) as way
      FROM planet_osm_polygon pp
      WHERE 
        landuse = 'residential'
      AND
        ST_Contains((SELECT ST_Buffer(geom, 100) FROM {STUDY_AREA_TBL} WHERE id = %s), pp.way)
    )
    INSERT INTO {RESIDENCES_TBL} (study_area_id, building, house_number, tags, geom)
    SELECT 
      {study_area_id}, p.building, p."addr:housenumber", p.tags, 
      ST_Centroid(p.way)
    FROM
      planet_osm_polygon p, boundary
    WHERE
      ST_Within(p.way, boundary.way)
    AND
      p.building IS NOT NULL
    '''

    cursor.execute(sql, (study_area_id, ))


def delete_residences(cursor, study_area_id: int) -> None:
    """removes all residences for a study area"""
    sql = f'DELETE FROM {RESIDENCES_TBL} WHERE study_area_id = %s'
    cursor.execute(sql, (study_area_id, ))


def _get_amenity_residence_distance_straight_top_three_sql() -> str:
    return f'''
    INSERT INTO {RES_AMENITY_DIST_STR_TBL} (residence_id, amenity_id, distance)
    SELECT rank_filter.residence_id, rank_filter.amenity_id, rank_filter.distance FROM (
        SELECT 
            re.id as residence_id, am.id as amenity_id, ST_Distance(am.geom, re.geom) as distance,
            rank() OVER (
                PARTITION BY re.id
                ORDER BY ST_Distance(am.geom, re.geom)
            )
        FROM
            {RESIDENCES_TBL} re, {AMENITIES_TBL} am
        WHERE
            am.name = %s AND am.category = %s
        AND
            building IN ('yes', 'house', 'residential', 'apartments', 'terrace', 'detached')
        AND
            re.study_area_id = %s AND am.study_area_id = %s
    ) rank_filter WHERE RANK <= 3;
    '''


def add_amenity_residence_distances_straight(cursor, study_area_id: int, amenities: list) -> Generator:
    """
    Finds the straight line distance amenity and residences.
    We only do this for the first three amenities that we find.
    """
    for amenity, category in amenities:
        sql = _get_amenity_residence_distance_straight_top_three_sql()
        yield cursor.execute(sql, (amenity, category, study_area_id, study_area_id))


def add_amenity_residence_distance(cursor, records: List[Tuple]) -> None:
    """
    adds network residence amenity distances

    tuple needs to be in the following order:
        distance, time, amenity_id, residence_id, mode
    """
    sql = f'''
        INSERT INTO 
            {RES_AMENITY_DIST_TBL} (distance, time, amenity_id, residence_id, mode)
        VALUES %s
    '''
    try:
        execute_values(
            cursor, sql, records, template=None, page_size=100
        )
    except Exception as exc:
        raise exc


def add_residence_amenity_category_distances(cursor, study_area_id: int, mode: str) -> None:
    """
    Inserts new records in the table holding the standardized scores for distance and time
    """
    sql = f'''
    INSERT INTO {RES_AMENITY_CAT_DIST_TBL}
        (residence_id, amenity_category, amenity_name, mode,
        average_distance, average_time, time_zscore, distance_zscore)
    SELECT 
        sub.residence_id, sub.amenity_category, sub.amenity_name, %s as mode, sub.avg_dist, sub.avg_time,
        (sub.avg_dist - AVG(sub.avg_dist) over(PARTITION BY sub.amenity_category, sub.amenity_name)) 
            / stddev_pop(sub.avg_dist) over(PARTITION BY sub.amenity_category, sub.amenity_name) as distance_zscore,
        (sub.avg_time - AVG(sub.avg_time) over(PARTITION BY sub.amenity_category, sub.amenity_name)) 
            / stddev_pop(sub.avg_time) over(PARTITION BY sub.amenity_category, sub.amenity_name) as time_zscore
    FROM (
        SELECT
            d.residence_id, am.category as amenity_category, am.name as amenity_name,
            CASE
                WHEN 
                    am.category = 'nature' OR am.category = 'school' 
                THEN
                    AVG(d.distance)
                ELSE
                    MIN(d.distance)
            END avg_dist,
            CASE
                WHEN 
                    am.category = 'nature' OR am.category = 'school' 
                THEN
                    AVG(d.time)
                ELSE
                    MIN(d.time)
            END avg_time
        FROM
            {RES_AMENITY_DIST_TBL} d
        JOIN
            {RESIDENCES_TBL} r
        ON
            d.residence_id = r.id
        JOIN
            {AMENITIES_TBL} am
        ON
            d.amenity_id = am.id
        WHERE
            r.study_area_id = %s AND am.study_area_id = %s
        AND
            d.mode = %s
        GROUP BY
            d.residence_id, am.category, am.name
    ) AS sub;
    '''

    cursor.execute(sql, (mode, study_area_id, study_area_id, mode))

