from tqdm import tqdm

from .read import get_amenity_names

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
        'emergency_service',
        'clinic'
    ),
    'community': (
        'community_centre',
        'social_facility',
        'grave_yard',
        'library',
        'arts_centre',
        'parish_hall'
    ),
    'outing_destination': (
        'pub',
        'biergarten',
        'cafe',
        'theatre',
        'nightclub',
        'bar',
        'ice_cream',
        'events_venue',
        'cinema',
        'restaurant',
        'fast_food'
    )
}

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
        planet_osm_point pp
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
        planet_osm_point pp
    WHERE 
        ST_Contains((SELECT geom from study_areas where id = %s), pp.way)
    AND
        shop IN ('{amenities}')
    '''
    cursor.execute(sql, (study_area_id, ))


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
        ST_Contains((SELECT geom from study_areas where id = %s), pp.way)
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


def _get_amenity_residence_distance_straight_sql(amenity):
    """get the SQL statement for the straight distance calculation"""
    return f'''
    INSERT INTO residence_amenity_distances_straight (residence_id, amenity_id)
    SELECT 
    re.id, (
        SELECT am.id
        FROM amenities am
        WHERE am.name = %s
        ORDER BY ST_Distance(re.geom, am.geom)
        LIMIT 1
    )
    FROM residences re
    WHERE
        "house_number" IS NOT NULL
    AND
        building IN ('yes', 'house', 'residential', 'apartments')
    AND
        study_area_id = %s;
    '''


def add_amenity_residence_distances_straight(cursor, study_area_id: int, show_status: bool = False) -> None:
    """
    Finds the straight line distance amenity and residences.
    We only do this for the first amenity that we find.
    """
    amenities = get_amenity_names(cursor, study_area_id)

    if show_status:
        amenities = tqdm(amenities, unit='amenity')

    for amenity in amenities:
        sql = _get_amenity_residence_distance_straight_sql(amenity)
        cursor.execute(sql, (amenity, study_area_id))
