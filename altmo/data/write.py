
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


def add_amenities(cursor, study_area_id: str) -> None:
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
        ST_Contains((SELECT geom from study_areas where name = 'kiel'), pp.way)
    AND
        amenity IN ('{amenities}')
    '''
    cursor.execute(sql)

    # Add polygons from amenities column
    sql = f'''
    INSERT INTO amenities (name, geom, study_area_id)
    SELECT 
        amenity, ST_Centroid(way), {study_area_id}
    FROM 
        planet_osm_point pp
    WHERE 
        ST_Contains((SELECT geom from study_areas where name = 'kiel'), pp.way)
    AND
        amenity IN ('{amenities}')
    '''
    cursor.execute(sql)

    # Add points from shop column
    sql = f'''
    INSERT INTO amenities (name, geom, study_area_id)
    SELECT 
        shop, way, {study_area_id}
    FROM 
        planet_osm_point pp
    WHERE 
        ST_Contains((SELECT geom from study_areas where name = 'kiel'), pp.way)
    AND
        shop IN ('{amenities}')
    '''
    cursor.execute(sql)

    # Add polygons from shop column
    sql = f'''
    INSERT INTO amenities (name, geom, study_area_id)
    SELECT 
        shop, ST_Centroid(way), {study_area_id}
    FROM 
        planet_osm_point pp
    WHERE 
        ST_Contains((SELECT geom from study_areas where name = 'kiel'), pp.way)
    AND
        shop IN ('{amenities}')
    '''
    cursor.execute(sql)


def add_amenities_category(cursor, study_area_id: str) -> None:
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
