from typing import List, Tuple

from tqdm import tqdm
from psycopg2.extras import execute_values

from .read import get_amenity_name_category

AMENITY_CATEGORIES = {
    'school': (
        'school',
        'kindergarten',
        'childcare',
        'university',
        'music_school',
        'driving_school',
        'college',
        'research_institute'
    ),
    'shopping': (
        'marketplace',
        'hairdresser',
        'clothes',
        'books',
        'florist',
        'optician',
        'furniture',
        'sports',
        'second_hand',
    ),
    'groceries': (
        'supermarket',
        'bakery',
        'butcher'
    ),
    'administrative': (
        'townhall',
        'police',
        'bank',
        'post_office',
        'post_box'
    ),
    'health': (
        'doctors',
        'hospital',
        'nursing_home',
        'veterinary',
        'pharmacy',
        'dentist',
        'clinic'
    ),
    'community': (
        'community_centre',
        'social_facility',
        'library',
    ),
    'outing_destination': (
        'pub',
        'cafe',
        'theatre',
        'nightclub',
        'bar',
        'ice_cream',
        'events_venue',
        'cinema',
        'restaurant',
        'fast_food'
    ),
}

NATURE_AMENITIES = (
    'park',
    'forest',
    'sports',
    'cemetery',
    'allotment'  # Schrebergartens fall into this category
)

AMENITY_CATEGORY_MAP = {
    amenity: category
    for category, amenities in AMENITY_CATEGORIES.items()
    for amenity in amenities
}


def delete_amenities(cursor, study_area_id: int) -> None:
    """removes all amenities for a study area"""
    sql = 'DELETE FROM amenities WHERE study_area_id = %s'
    cursor.execute(sql, (study_area_id, ))


def add_amenities(cursor, study_area_id: int) -> None:
    """adds amenities to the amenities table without category"""
    amenities = "','".join(AMENITY_CATEGORY_MAP.keys())

    # Add points from amenities column
    sql = f'''
    INSERT INTO amenities (name, geom, study_area_id)
    SELECT 
        amenity, way, {study_area_id}
    FROM 
        planet_osm_point pp
    WHERE 
        ST_Contains((SELECT geom from study_areas where id = %s), pp.way)
    AND
        amenity IN ('{amenities}')
    '''
    cursor.execute(sql, (study_area_id, ))

    # Add polygons from amenities column
    sql = f'''
    INSERT INTO amenities (name, geom, study_area_id)
    SELECT 
        amenity, ST_Centroid(way), {study_area_id}
    FROM 
        planet_osm_polygon pp
    WHERE 
        ST_Contains((SELECT geom from study_areas where id = %s), pp.way)
    AND
        amenity IN ('{amenities}')
    '''
    cursor.execute(sql, (study_area_id, ))

    # Add points from shop column
    sql = f'''
    INSERT INTO amenities (name, geom, study_area_id)
    SELECT 
        shop, way, {study_area_id}
    FROM 
        planet_osm_point pp
    WHERE 
        ST_Contains((SELECT geom from study_areas where id = %s), pp.way)
    AND
        shop IN ('{amenities}')
    '''
    cursor.execute(sql, (study_area_id, ))

    # Add polygons from shop column
    sql = f'''
    INSERT INTO amenities (name, geom, study_area_id)
    SELECT 
        shop, ST_Centroid(way), {study_area_id}
    FROM 
        planet_osm_polygon pp
    WHERE 
        ST_Contains((SELECT geom from study_areas where id = %s), pp.way)
    AND
        shop IN ('{amenities}')
    '''
    cursor.execute(sql, (study_area_id, ))


def add_natural_amenities(cursor, study_area_id: int) -> None:
    """runs special queries that add natural amenities for a study area"""
    sql = '''
    SELECT
        (ST_Dump(ST_GeneratePoints(pp.way, (ceil(pp.way_area/50000.0))::integer))).geom, 
        landuse, leisure, "natural"
    FROM
        planet_osm_polygon pp
    WHERE
        ST_Contains((SELECT geom from study_areas where id = %s), pp.way)
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

    insert_sql = '''
        INSERT INTO amenities (geom, category, name, study_area_id) VALUES %s
    '''
    execute_values(
        cursor, insert_sql, records, template=None, page_size=100
    )


def add_amenities_category(cursor, study_area_id: int) -> None:
    values = AMENITY_CATEGORY_MAP.items()
    values_str = ','.join([
        f"('{name}', '{category}')"
        for name, category in values
    ])

    sql = f'''
    UPDATE amenities as a set
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
        ST_Contains((SELECT ST_Buffer(geom, 100) from study_areas where id = %s), pp.way)
    )
    INSERT INTO residences (study_area_id, building, house_number, tags, geom)
    SELECT 
      {study_area_id}, p.building, p."addr:housenumber", p.tags, 
      ST_Centroid(p.way)
    FROM
      planet_osm_polygon p, boundary
    WHERE
      ST_Within(p.way, boundary.way)
    AND
      p.building IS NOT NULL;
    '''

    cursor.execute(sql, (study_area_id, ))


def delete_residences(cursor, study_area_id: int) -> None:
    """removes all residences for a study area"""
    sql = 'DELETE FROM residences WHERE study_area_id = %s'
    cursor.execute(sql, (study_area_id, ))


def _get_amenity_residence_distance_straight_sql() -> str:
    """get the SQL statement for the straight distance calculation"""
    return f'''
    INSERT INTO residence_amenity_distances_straight (residence_id, amenity_id)
    SELECT 
    re.id, (
        SELECT am.id
        FROM amenities am
        WHERE am.name = %s AND category = %s
        ORDER BY ST_Distance(re.geom, am.geom)
        LIMIT 1
    )
    FROM 
        residences re
    WHERE
        building IN ('yes', 'house', 'residential', 'apartments', 'terrace', 'detached')
    AND
        study_area_id = %s;
    '''


def _get_amenity_residence_distance_straight_top_three_sql() -> str:
    return '''
    INSERT INTO residence_amenity_distances_straight (residence_id, amenity_id)
    SELECT rank_filter.residence_id, rank_filter.amenity_id FROM (
        SELECT 
            re.id as residence_id, am.id as amenity_id, ST_Distance(am.geom, re.geom),
            rank() OVER (
                PARTITION BY re.id
                ORDER BY ST_Distance(am.geom, re.geom)
            )
        FROM
            residences re, amenities am
        WHERE
            am.name = %s AND am.category = %s
        AND
            building IN ('yes', 'house', 'residential', 'apartments', 'terrace', 'detached')
        AND
            re.study_area_id = %s AND am.study_area_id = %s
    ) rank_filter WHERE RANK <= 3;
    '''


def add_amenity_residence_distances_straight(
        cursor, study_area_id: int, category: str = None, show_status: bool = False) -> None:
    """
    Finds the straight line distance amenity and residences.
    We only do this for the first amenity that we find.
    """
    amenities = get_amenity_name_category(cursor, study_area_id, category=category)

    if show_status:
        amenities = tqdm(amenities, unit='amenity')

    for amenity, category in amenities:
        sql = _get_amenity_residence_distance_straight_top_three_sql()
        cursor.execute(sql, (amenity, category, study_area_id, study_area_id))


def add_amenity_residence_distance(cursor, records: List[Tuple]) -> None:
    """
    adds network residence amenity distances

    tuple needs to be in the following order:
        distance, time, amenity_id, residence_id, mode
    """
    sql = '''
        INSERT INTO 
            residence_amenity_distances (distance, time, amenity_id, residence_id, mode)
        VALUES %s
    '''
    execute_values(
        cursor, sql, records, template=None, page_size=100
    )


def add_standardized_network_distances(cursor, study_area_id: int) -> None:
    """
    Creates a new table holding the standardized scores for distance and time
    """
    sql = '''
    INSERT INTO  residence_amenity_distance_standardized 
        (residence_id, amenity_category, amenity_name, average_time, average_distance, time_zscore, distance_zscore)
    SELECT 
        sub.residence_id, sub.amenity_category, sub.amenity_name, sub.avg_dist, sub.avg_time,
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
            residence_amenity_distances d
        JOIN
            residences r
        ON
            d.residence_id = r.id
        JOIN
            amenities am
        ON
            d.amenity_id = am.id
        WHERE
            r.study_area_id = %s AND am.study_area_id = %s
        GROUP BY
            d.residence_id, am.category, am.name
    ) AS sub;
    '''

    cursor.execute(sql, (study_area_id, study_area_id))

