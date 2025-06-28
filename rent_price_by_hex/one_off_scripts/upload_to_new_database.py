from connection import hd_connection, local_db_connection

if __name__ == '__main__':
    # Step 1: Table creation SQL
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS unit_prices_summary (
        id uuid NOT NULL PRIMARY KEY,
        state              CHAR(2),
        city               TEXT,
        lat                DOUBLE PRECISION,
        lon                DOUBLE PRECISION,
        number_units       INTEGER,
        unit_mix_new       TEXT,
        is_student         BOOLEAN,
    
        price_per_bed      NUMERIC(10, 2),
        average_price_per_unit NUMERIC(10, 2),
        avg_num_beds       NUMERIC(10, 2),
        avg_num_baths      NUMERIC(10, 2),
        avg_baths_per_bed  NUMERIC(10, 2),
    
        avg_price_bed1     NUMERIC(10, 2),
        avg_price_bed2     NUMERIC(10, 2),
        avg_price_bed3     NUMERIC(10, 2),
        avg_price_bed4     NUMERIC(10, 2),
        avg_price_bed5     NUMERIC(10, 2),
        avg_price_bed6     NUMERIC(10, 2)
    );
    """

    # Step 2: Data extraction query
    data_query = """
    SELECT
        b.id,
        ANY_VALUE(b.state)          AS state,
        ANY_VALUE(b.city)           AS city,
        ANY_VALUE(b.lat)            AS lat,
        ANY_VALUE(b.lon)            AS lon,
        ANY_VALUE(b.number_units)   AS number_units,
        ANY_VALUE(b.unit_mix_new)   AS unit_mix_new,
        ANY_VALUE(b.is_student)     AS is_student,
    
        SUM(u.effective_price) / SUM(u.bed) AS price_per_bed,
    
        ROUND(AVG(u.effective_price), 2)                            AS average_price_per_unit,
        ROUND(AVG(u.bed),            2)                             AS avg_num_beds,
        ROUND(AVG(u.bath),           2)                             AS avg_num_baths,
        ROUND(AVG(u.bath::numeric / u.bed), 2)                      AS avg_baths_per_bed,
    
        ROUND(AVG(CASE WHEN u.bed = 1 THEN u.effective_price END), 2) AS avg_price_bed1,
        ROUND(AVG(CASE WHEN u.bed = 2 THEN u.effective_price END), 2) AS avg_price_bed2,
        ROUND(AVG(CASE WHEN u.bed = 3 THEN u.effective_price END), 2) AS avg_price_bed3,
        ROUND(AVG(CASE WHEN u.bed = 4 THEN u.effective_price END), 2) AS avg_price_bed4,
        ROUND(AVG(CASE WHEN u.bed = 5 THEN u.effective_price END), 2) AS avg_price_bed5,
        ROUND(AVG(CASE WHEN u.bed = 6 THEN u.effective_price END), 2) AS avg_price_bed6
    FROM   buildings b
    JOIN   units u ON u.building_id = b.id
    WHERE  b.is_single_family = FALSE
      AND  b.created_on > DATE '2025-01-01'
      AND  (u.exit_market IS NULL OR u.exit_market > DATE '2025-01-01')
      AND  u.bath > 0
      AND  u.bed  > 0
    GROUP BY b.id
    """

    # Step 3: Download data from hd_connection
    with hd_connection() as source_conn:
        with source_conn.cursor() as source_cursor:
            source_cursor.execute(data_query)
            rows = source_cursor.fetchall()
            columns = [desc[0] for desc in source_cursor.description]
            print(f"Downloaded {len(rows)} rows from HD connection.")

    # Step 4: Upload data to local_db_connection
    with local_db_connection() as dest_conn:
        with dest_conn.cursor() as dest_cursor:
            # Create table if not exists
            dest_cursor.execute(create_table_sql)
            print("Created table unit_prices_summary in local database.")

            # Insert data
            for row in rows:
                placeholders = ', '.join(['%s'] * len(row))
                column_list = ', '.join(columns)
                insert_stmt = f"INSERT INTO unit_prices_summary ({column_list}) VALUES ({placeholders})"
                dest_cursor.execute(insert_stmt, row)

            dest_conn.commit()
            print(f"Inserted {len(rows)} rows into local database.")
