from altmo.settings import PG_DSN, SRS_ID, TBL_PREFIX

from .decorators import psycopg2_cur

TBL_PREFIX = TBL_PREFIX
STUDY_AREA_TBL = f'{TBL_PREFIX}study_areas'
STUDY_PARTS_TBL = f'{TBL_PREFIX}study_area_parts'
AMENITIES_TBL = f'{TBL_PREFIX}amenities'
RESIDENCES_TBL = f'{TBL_PREFIX}residences'
RES_AMENITY_DIST_TBL = f'{TBL_PREFIX}residence_amenity_distances'
RES_AMENITY_DIST_STR_TBL = f'{TBL_PREFIX}residence_amenity_distances_straight'
RES_AMENITY_CAT_DIST_TBL = f'{TBL_PREFIX}residence_amenity_category_distances'
RES_AMENITY_STD_CAT_TBL = f'{TBL_PREFIX}residence_amenity_distance_standardized_categorized'


@psycopg2_cur(PG_DSN)
def create_schema(cursor):
    study_areas_sql = f'''
        CREATE TABLE {STUDY_AREA_TBL} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) UNIQUE,
            description TEXT,
            geom Geometry(Geometry, {SRS_ID})
        )
    '''

    study_areas_parts_sql = f'''
        CREATE TABLE {STUDY_PARTS_TBL} (
            id SERIAL PRIMARY KEY,
            study_area_id INTEGER REFERENCES {STUDY_AREA_TBL}(id),
            name VARCHAR(100) UNIQUE,
            description TEXT,
            geom Geometry(Geometry, {SRS_ID})
        )
    '''

    amenities_sql = f'''
        CREATE TABLE {AMENITIES_TBL} (
            id SERIAL PRIMARY KEY,
            study_area_id INTEGER REFERENCES {STUDY_AREA_TBL}(id),
            name VARCHAR(200),
            category VARCHAR(100),
            geom Geometry(Point, {SRS_ID})
        )
    '''

    residences_sql = f'''
        CREATE TABLE {RESIDENCES_TBL} (
            id SERIAL PRIMARY KEY,
            study_area_id INTEGER REFERENCES {STUDY_AREA_TBL}(id),
            tags HSTORE,
            house_number VARCHAR(100),
            building VARCHAR(100),
            geom Geometry(Point, {SRS_ID})
        )
    '''

    residence_amenity_distances_sql = f'''
        CREATE TABLE {RES_AMENITY_DIST_TBL} (
            residence_id INTEGER REFERENCES {RESIDENCES_TBL}(id),
            amenity_id INTEGER REFERENCES {AMENITIES_TBL}(id),
            distance FLOAT,
            time BIGINT,
            mode VARCHAR(10),
            PRIMARY KEY (residence_id, amenity_id, mode)
        )
    '''

    residence_amenity_distances_straight_sql = f'''
        CREATE TABLE {RES_AMENITY_DIST_STR_TBL} (
            residence_id INTEGER REFERENCES {RESIDENCES_TBL}(id),
            amenity_id INTEGER REFERENCES {AMENITIES_TBL}(id),
            distance FLOAT,
            PRIMARY KEY (residence_id, amenity_id)
        )
    '''

    residence_amenity_standardized_sql = f'''
        CREATE TABLE {RES_AMENITY_CAT_DIST_TBL} (
            residence_id INTEGER REFERENCES {RESIDENCES_TBL}(id),
            amenity_category VARCHAR(100),
            amenity_name VARCHAR(100),
            time_zscore FLOAT,
            distance_zscore FLOAT,
            average_time FLOAT,
            average_distance FLOAT,
            mode VARCHAR(10),
            PRIMARY KEY (residence_id, amenity_category, amenity_name, mode)
        )
    '''

    residence_amenity_standardized_categorized_sql = f'''
        CREATE TABLE {RES_AMENITY_STD_CAT_TBL} (
            residence_id INTEGER REFERENCES {RESIDENCES_TBL}(id),
            administrative_time_zscore FLOAT,
            community_time_zscore FLOAT,
            groceries_time_zscore FLOAT,
            health_time_zscore FLOAT,
            nature_time_zscore FLOAT,
            outing_destination_time_zscore FLOAT,
            school_time_zscore FLOAT,
            shopping_time_zscore FLOAT,
            all_time_zscore FLOAT,
            administrative_average_time FLOAT,
            community_average_time FLOAT,
            groceries_average_time FLOAT,
            health_average_time FLOAT,
            nature_average_time FLOAT,
            outing_destination_average_time FLOAT,
            school_average_time FLOAT,
            shopping_average_time FLOAT,
            all_average_time FLOAT,
            mode VARCHAR(10),
            PRIMARY KEY (residence_id, mode)
        )
    '''

    cursor.execute(study_areas_sql)
    cursor.execute(study_areas_parts_sql)
    cursor.execute(amenities_sql)
    cursor.execute(residences_sql)
    cursor.execute(residence_amenity_distances_sql)
    cursor.execute(residence_amenity_distances_straight_sql)
    cursor.execute(residence_amenity_standardized_sql)
    cursor.execute(residence_amenity_standardized_categorized_sql)


@psycopg2_cur(PG_DSN)
def remove_schema(cursor):
    cursor.execute(f'DROP TABLE {RES_AMENITY_CAT_DIST_TBL} CASCADE')
    cursor.execute(f'DROP TABLE {RES_AMENITY_STD_CAT_TBL} CASCADE')
    cursor.execute(f'DROP TABLE {RES_AMENITY_DIST_TBL} CASCADE')
    cursor.execute(f'DROP TABLE {RES_AMENITY_DIST_STR_TBL} CASCADE')
    cursor.execute(f'DROP TABLE {STUDY_PARTS_TBL} CASCADE')
    cursor.execute(f'DROP TABLE {STUDY_AREA_TBL} CASCADE')
    cursor.execute(f'DROP TABLE {AMENITIES_TBL} CASCADE')
    cursor.execute(f'DROP TABLE {RESIDENCES_TBL} CASCADE')
