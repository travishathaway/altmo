from typing import List, Tuple, Dict

from altmo.utils import get_category_amenity_keys
from altmo.settings import TABLES


def get_study_area(cursor, name) -> tuple:
    """returns a single study area"""
    cursor.execute(
        f"SELECT id, name, description, geom FROM {TABLES.STUDY_AREA_TBL} WHERE name = %s",
        (name,),
    )

    return cursor.fetchone() or (None, None)


def get_amenity_name_category(
    cursor, study_area_id: int, category=None, name=None
) -> List[tuple]:
    """return all the amenity category, amenity name pairs for a single study_area"""
    sql = (
        f"SELECT DISTINCT name, category FROM {TABLES.AMENITIES_TBL} WHERE study_area_id = %s"
    )
    params = (study_area_id,)

    if category:
        sql += " AND category = %s"
        params += (category,)

    if name:
        sql += " AND name = %s"
        params += (name,)

    cursor.execute(sql, params)
    return cursor.fetchall()


def get_study_area_residences(cursor, study_area_id: int) -> List[Tuple]:
    """fetch all residences for a study area"""
    sql = f"""
        SELECT
            id, ST_X(ST_Transform(geom, 4326)), ST_Y(ST_Transform(geom, 4326))
        FROM
            {TABLES.RESIDENCES_TBL}
        WHERE study_area_id = %s
    """
    cursor.execute(sql, (study_area_id,))
    return cursor.fetchall()


def get_residence_amenity_straight_distance(
    cursor, study_area_id: int, /, *,
    start: int = 0, limit: int = 1000, srs_id: int = 4326,
    category: str = None, name: str = None
) -> List[Tuple]:
    params = {
        'study_area_id': study_area_id,
        'srs_id': srs_id,
        'start': start,
        'limit': limit,
    }

    extra_where_sql = ''
    if category is not None:
        extra_where_sql += " AND am.category = %(category)s"
        params['category'] = category

    if name is not None:
        extra_where_sql += " AND am.name = %(name)s"
        params['name'] = name

    sql = f"""
    SELECT
        residence_id,
        amenity_id, 
        ST_Y(ST_Transform(r.geom, %(srs_id)s)) as residence_lat,
        ST_X(ST_Transform(r.geom, %(srs_id)s)) as residence_lng,
        ST_Y(ST_Transform(am.geom, %(srs_id)s)) as amenity_lat,
        ST_X(ST_Transform(am.geom, %(srs_id)s)) as amenity_lng
    FROM
        {TABLES.RES_AMENITY_DIST_STR_TBL}
    JOIN
        {TABLES.AMENITIES_TBL} am
    ON
        amenity_id = am.id
    JOIN
        {TABLES.RESIDENCES_TBL} r
    ON
        residence_id = r.id
    WHERE
        r.study_area_id = %(study_area_id)s
    {extra_where_sql}
    ORDER BY
        r.id
    OFFSET %(start)s
    LIMIT %(limit)s
    """

    cursor.execute(sql, params)
    return cursor.fetchall()


def get_residence_amenity_straight_distance_count(cursor, study_area_id: int) -> int:
    """Gets the count of the current number of records in the straight distances table for a study_area_id"""
    sql = f"""
    SELECT
        count(*)
    FROM
        {TABLES.RES_AMENITY_DIST_STR_TBL}
    JOIN
        {TABLES.RESIDENCES_TBL} r
    ON
        residence_id = r.id
    WHERE
        r.study_area_id = %s
    """
    params = (study_area_id, )
    cursor.execute(sql, params)

    result = cursor.fetchone()

    return result[0] if result else 0


async def get_residence_amenity_straight_distance_async(
    cursor, study_area_id: int, /, *,
    start: int = 0, limit: int = 1000, srs_id: int = 4326,
    category: str = None, name: str = None
) -> List[Tuple]:
    params = {
        'study_area_id': study_area_id,
        'srs_id': srs_id,
        'start': start,
        'limit': limit,
    }

    extra_where_sql = ''
    if category is not None:
        extra_where_sql += " AND am.category = %s"
        params += (category,)

    if name is not None:
        extra_where_sql += " AND am.name = %s"
        params += (name,)

    sql = f"""
    SELECT
        residence_id,
        amenity_id, 
        ST_Y(ST_Transform(r.geom, %(srs_id)s)) as residence_lat,
        ST_X(ST_Transform(r.geom, %(srs_id)s)) as residence_lng,
        ST_Y(ST_Transform(am.geom, %(srs_id)s)) as amenity_lat,
        ST_X(ST_Transform(am.geom, %(srs_id)s)) as amenity_lng
    FROM
        {TABLES.RES_AMENITY_DIST_STR_TBL}
    JOIN
        {TABLES.AMENITIES_TBL} am
    ON
        amenity_id = am.id
    JOIN
        {TABLES.RESIDENCES_TBL} r
    ON
        residence_id = r.id
    WHERE
        r.study_area_id = %(study_area_id)s
    {extra_where_sql}
    ORDER BY
        r.id
    OFFSET %(start)s
    LIMIT %(limit)s
    """

    await cursor.execute(sql, params)
    result = await cursor.fetchall()
    return result


def _get_residence_composite_average_times_sql(
    study_area_id: int,
    mode: str,
    weights: Dict[str, Dict],
    amenities: List[tuple],
    include_geojson=False,
    srs_id=3857,
) -> Tuple[tuple, str]:
    """
    Retrieves a list of residences with their composite averages based on amenities config.

    This function builds a rather complex SQL query (using the `crosstab` function).

    Admittedly it is a bit messy. One thing to watch out for is the `sub_sql` string
    which has to be escaped because it is being passed to the `crosstab` function.

    VULNERABLE TO SQL INJECTION !!! (Never use this function with untrusted inputs!!!)

    :param study_area_id: Used to narrow our query to study area we are interested in
    :param mode: Used to limit the results to a mode a transport ('pedestrian' or 'bicycle')
    :param weights: Mapping used to pass in weighting information (important for the composite index), but
                    also lets the function know which category amenity pairs to retrieve
    :param amenities: Amenities currently stored in database for a study area
    :param include_geojson: Optionally include residence geometry column

    :returns: a tuple containing the columns and the SQL query string
    """
    config_cat_amts = get_category_amenity_keys(weights)

    # if it is not in our database or the config file, we do not use it
    amenity_categories = sorted(
        [
            f"{category}_{amenity}"
            for amenity, category in amenities
            if f"{category}_{amenity}" in config_cat_amts
        ]
    )
    cat_amt_filter_str = "'', ''".join(amenity_categories)

    sub_sql = f"""
    SELECT
        ra.residence_id as id, a.category || ''_'' || a.name as category, avg(ra.time) as avg_time
    FROM
        {TABLES.RES_AMENITY_DIST_TBL} ra
    LEFT JOIN
        {TABLES.AMENITIES_TBL} a
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
    """

    distinct_sql = f"""
    SELECT
        DISTINCT a.category || ''_'' || a.name AS category
    FROM
        {TABLES.RES_AMENITY_DIST_TBL} ra
    LEFT JOIN
        {TABLES.AMENITIES_TBL} a
    ON
        a.id = ra.amenity_id
    WHERE
        a.category || ''_'' || a.name IN (''{cat_amt_filter_str}'')
    ORDER BY
        a.category || ''_'' || a.name
    """

    # Define the columns we extract from the `crosstab` function
    pivot_columns_with_type = [f"{col} NUMERIC" for col in amenity_categories]
    pivot_columns_str = ", ".join(pivot_columns_with_type)

    def get_category_avg_stmts_str() -> str:
        cat_avg_stmts: List[str] = []

        for category, amts in weights.items():
            wght_stmts = []
            for amenity, wght in amts.items():
                if (amenity, category) in [(a, c) for a, c in amenities]:
                    wght_stmts.append(f'{category}_{amenity} * {wght["weight"]}')
            wght_str = "+ ".join(wght_stmts)
            cat_avg_stmts.append(f"({wght_str}) as {category}")

        return ",".join(cat_avg_stmts)

    # Used for the top level select in our query
    categories = {c for _, c in amenities if c in weights}
    categories_str = ", ".join(categories)

    all_avg_str = ""
    if len(amenities) > 0:
        factor = 1.0 / len(categories)
        all_avg_str = "+ ".join([f"{c} * {factor}" for c in categories])
        all_avg_str = f" ({all_avg_str}) as all"

    cols = (
        "residence_id",
        "all",
    ) + tuple(categories)
    cols_str = f"residence_id, {all_avg_str}, {categories_str}"
    join_stmt = ""

    if include_geojson:
        cols += ("geom",)
        cols_str += f", ST_AsGeoJSON(ST_Transform(r.geom, {srs_id}))"
        join_stmt = f"LEFT JOIN {TABLES.RESIDENCES_TBL} r ON r.id = sub.residence_id"

    sql = f"""
    SELECT
        {cols_str}
    FROM (
        SELECT
            residence_id, {get_category_avg_stmts_str()}
        FROM
            crosstab('{sub_sql}', '{distinct_sql}')
        AS (
            residence_id INTEGER, {pivot_columns_str}
        )
    ) as sub
    {join_stmt}
    """

    return cols, sql


def get_residence_composite_average_times(
    cursor,
    study_area_id: int,
    mode: str,
    weights: Dict[str, Dict],
    include_geojson=False,
    srs_id=3857,
) -> Tuple[tuple, List[tuple]]:
    """
    Get the residence composites for each residence in a provided study area

    :returns: the column names and the raw data from the fetch
    """
    amenities = get_amenity_name_category(cursor, study_area_id)
    cols, sql = _get_residence_composite_average_times_sql(
        study_area_id,
        mode,
        weights,
        amenities,
        include_geojson=include_geojson,
        srs_id=srs_id,
    )
    cursor.execute(sql)

    return cols, cursor.fetchall()
