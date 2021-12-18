from altmo.settings import get_config, TABLES

from .decorators import psycopg2_cur


@psycopg2_cur()
@get_config
def create_schema(config, cursor):
    study_areas_sql = f"""
        CREATE TABLE {TABLES.STUDY_AREA_TBL} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) UNIQUE,
            description TEXT,
            geom Geometry(Geometry, {config.SRS_ID})
        )
    """

    study_areas_parts_sql = f"""
        CREATE TABLE {TABLES.STUDY_PARTS_TBL} (
            id SERIAL PRIMARY KEY,
            study_area_id INTEGER REFERENCES {TABLES.STUDY_AREA_TBL}(id),
            name VARCHAR(100) UNIQUE,
            description TEXT,
            geom Geometry(Geometry, {config.SRS_ID})
        )
    """

    amenities_sql = f"""
        CREATE TABLE {TABLES.AMENITIES_TBL} (
            id SERIAL PRIMARY KEY,
            study_area_id INTEGER REFERENCES {TABLES.STUDY_AREA_TBL}(id),
            name VARCHAR(200),
            category VARCHAR(100),
            geom Geometry(Point, {config.SRS_ID})
        )
    """

    residences_sql = f"""
        CREATE TABLE {TABLES.RESIDENCES_TBL} (
            id SERIAL PRIMARY KEY,
            study_area_id INTEGER REFERENCES {TABLES.STUDY_AREA_TBL}(id),
            tags HSTORE,
            house_number VARCHAR(100),
            building VARCHAR(100),
            geom Geometry(Point, {config.SRS_ID})
        )
    """

    residence_amenity_distances_sql = f"""
        CREATE TABLE {TABLES.RES_AMENITY_DIST_TBL} (
            residence_id INTEGER REFERENCES {TABLES.RESIDENCES_TBL}(id),
            amenity_id INTEGER REFERENCES {TABLES.AMENITIES_TBL}(id),
            distance FLOAT,
            time BIGINT,
            mode VARCHAR(10),
            PRIMARY KEY (residence_id, amenity_id, mode)
        )
    """

    residence_amenity_distances_straight_sql = f"""
        CREATE TABLE {TABLES.RES_AMENITY_DIST_STR_TBL} (
            residence_id INTEGER REFERENCES {TABLES.RESIDENCES_TBL}(id),
            amenity_id INTEGER REFERENCES {TABLES.AMENITIES_TBL}(id),
            distance FLOAT,
            PRIMARY KEY (residence_id, amenity_id)
        )
    """

    residence_amenity_standardized_sql = f"""
        CREATE TABLE {TABLES.RES_AMENITY_CAT_DIST_TBL} (
            residence_id INTEGER REFERENCES {TABLES.RESIDENCES_TBL}(id),
            amenity_category VARCHAR(100),
            amenity_name VARCHAR(100),
            time_zscore FLOAT,
            distance_zscore FLOAT,
            average_time FLOAT,
            average_distance FLOAT,
            mode VARCHAR(10),
            PRIMARY KEY (residence_id, amenity_category, amenity_name, mode)
        )
    """

    cursor.execute(study_areas_sql)
    cursor.execute(study_areas_parts_sql)
    cursor.execute(amenities_sql)
    cursor.execute(residences_sql)
    cursor.execute(residence_amenity_distances_sql)
    cursor.execute(residence_amenity_distances_straight_sql)
    cursor.execute(residence_amenity_standardized_sql)


@psycopg2_cur()
def remove_schema(cursor):
    cursor.execute(f"DROP TABLE {TABLES.RES_AMENITY_CAT_DIST_TBL} CASCADE")
    cursor.execute(f"DROP TABLE {TABLES.RES_AMENITY_DIST_TBL} CASCADE")
    cursor.execute(f"DROP TABLE {TABLES.RES_AMENITY_DIST_STR_TBL} CASCADE")
    cursor.execute(f"DROP TABLE {TABLES.STUDY_PARTS_TBL} CASCADE")
    cursor.execute(f"DROP TABLE {TABLES.STUDY_AREA_TBL} CASCADE")
    cursor.execute(f"DROP TABLE {TABLES.AMENITIES_TBL} CASCADE")
    cursor.execute(f"DROP TABLE {TABLES.RESIDENCES_TBL} CASCADE")
