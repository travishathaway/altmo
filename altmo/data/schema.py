from altmo.settings import PG_DSN

from .decorators import psycopg2_cur

SRS_ID = '3857'


@psycopg2_cur(PG_DSN)
def create_schema(cursor):
    study_areas_sql = f'''
        CREATE TABLE study_areas (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) UNIQUE,
            description TEXT,
            geom Geometry(MultiPolygon, {SRS_ID})
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

    traffic_flow_sql = f'''
        CREATE TABLE traffic_flow (
            id SERIAL PRIMARY KEY,
            flow_reading VARCHAR(100),
            flow_data JSONB,
            geom Geometry(Point, {SRS_ID})
        )
    '''

    pop_density_sql = f'''
        CREATE TABLE pop_density (
            id SERIAL PRIMARY KEY,
            num_people INTEGER,
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
            time INTEGER,
            mode VARCHAR(10),
            PRIMARY KEY (residence_id, amenity_id)
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
        CREATE TABLE residence_amenity_standardized (
            residence_id INTEGER REFERENCES residences(id),
            amenity_id INTEGER REFERENCES amenities(id),
            study_area_id INTEGER REFERENCES study_areas(id),
            z_score FLOAT,
            rank FLOAT,
            PRIMARY KEY (residence_id, amenity_id, study_area_id)
        )
    '''

    cursor.execute(study_areas_sql)
    cursor.execute(amenities_sql)
    cursor.execute(traffic_flow_sql)
    cursor.execute(pop_density_sql)
    cursor.execute(residences_sql)
    cursor.execute(residence_amenity_distances_sql)
    cursor.execute(residence_amenity_distances_straight_sql)
    cursor.execute(residence_amenity_standardized_sql)


@psycopg2_cur(PG_DSN)
def remove_schema(cursor):
    cursor.execute('DROP TABLE residence_amenity_standardized CASCADE')
    cursor.execute('DROP TABLE residence_amenity_distances CASCADE')
    cursor.execute('DROP TABLE residence_amenity_distances_straight CASCADE')
    cursor.execute('DROP TABLE study_areas CASCADE')
    cursor.execute('DROP TABLE amenities CASCADE')
    cursor.execute('DROP TABLE traffic_flow CASCADE')
    cursor.execute('DROP TABLE pop_density CASCADE')
    cursor.execute('DROP TABLE residences CASCADE')
