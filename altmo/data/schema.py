from altmo.settings import PG_DSN, SRS_ID

from .decorators import psycopg2_cur


@psycopg2_cur(PG_DSN)
def create_schema(cursor):
    study_areas_sql = f'''
        CREATE TABLE study_areas (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) UNIQUE,
            description TEXT,
            geom Geometry(Geometry, {SRS_ID})
        )
    '''

    study_areas_parts_sql = f'''
        CREATE TABLE study_area_parts (
            id SERIAL PRIMARY KEY,
            study_area_id INTEGER REFERENCES study_areas(id),
            name VARCHAR(100) UNIQUE,
            description TEXT,
            geom Geometry(Geometry, {SRS_ID})
        )
    '''

    amenities_sql = f'''
        CREATE TABLE amenities (
            id SERIAL PRIMARY KEY,
            study_area_id INTEGER REFERENCES study_areas(id),
            name VARCHAR(200),
            category VARCHAR(100),
            geom Geometry(Point, {SRS_ID})
        )
    '''

    residences_sql = f'''
        CREATE TABLE residences (
            id SERIAL PRIMARY KEY,
            study_area_id INTEGER REFERENCES study_areas(id),
            tags HSTORE,
            house_number VARCHAR(100),
            building VARCHAR(100),
            geom Geometry(Point, {SRS_ID})
        )
    '''

    residence_amenity_distances_sql = f'''
        CREATE TABLE residence_amenity_distances (
            residence_id INTEGER REFERENCES residences(id),
            amenity_id INTEGER REFERENCES amenities(id),
            distance FLOAT,
            time BIGINT,
            mode VARCHAR(10),
            PRIMARY KEY (residence_id, amenity_id, mode)
        )
    '''

    residence_amenity_distances_straight_sql = f'''
        CREATE TABLE residence_amenity_distances_straight (
            residence_id INTEGER REFERENCES residences(id),
            amenity_id INTEGER REFERENCES amenities(id),
            PRIMARY KEY (residence_id, amenity_id)
        )
    '''

    residence_amenity_standardized_sql = f'''
        CREATE TABLE residence_amenity_distance_standardized (
            residence_id INTEGER REFERENCES residences(id),
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
        CREATE TABLE residence_amenity_distance_standardized_categorized (
            residence_id INTEGER REFERENCES residences(id),
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
    cursor.execute('DROP TABLE residence_amenity_standardized_categorized CASCADE')
    cursor.execute('DROP TABLE residence_amenity_standardized CASCADE')
    cursor.execute('DROP TABLE residence_amenity_distances CASCADE')
    cursor.execute('DROP TABLE residence_amenity_distances_straight CASCADE')
    cursor.execute('DROP TABLE study_area_parts CASCADE')
    cursor.execute('DROP TABLE study_areas CASCADE')
    cursor.execute('DROP TABLE amenities CASCADE')
    cursor.execute('DROP TABLE residences CASCADE')
