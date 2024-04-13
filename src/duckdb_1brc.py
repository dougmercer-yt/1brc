import duckdb

with duckdb.connect() as conn:
    data = conn.sql(
        """SELECT
            station_name,
            MIN(measurement) AS min_measurement,
            CAST(AVG(measurement) AS DECIMAL(8,1)) AS mean_measurement,
            MAX(measurement) AS max_measurement
        FROM READ_CSV(
            'data/measurements.txt',
            header=false,
            columns={'station_name':'VARCHAR','measurement':'DECIMAL(8,1)'},
            delim=';',
            parallel=true
        )
        GROUP BY station_name"""
    )

    print(
        "{",
        ", ".join(f"{row[0]}={row[1]}/{row[2]}/{row[3]}" for row in sorted(data.fetchall())),
        "}",
        sep="",
    )
