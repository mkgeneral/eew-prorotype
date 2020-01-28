# DROP TABLES

warning_table_drop = "DROP table IF EXISTS warnings"
peak_accel_table_drop = "DROP table IF EXISTS peak_accel"


## CREATE TABLES

warning_table_create = ("""CREATE TABLE IF NOT EXISTS warnings (
								id 						SERIAL PRIMARY KEY,
                                device_id 				VARCHAR(5) NOT NULL, 
                                warning_acceleration 	NUMERIC,
                                warning_time			TIMESTAMP,
                                ingest_time   			TIMESTAMP
                    );""")

peak_accel_table_create = ("""CREATE TABLE IF NOT EXISTS peak_accel ( 
								id 						SERIAL PRIMARY KEY,
                                device_id 				VARCHAR(5) NOT NULL, 
                                peak_acceleration    	NUMERIC,
                                acceleration_time		TIMESTAMP,
                                ingest_time   			TIMESTAMP
                    );""")


## INSERT RECORDS

warning_table_insert = ("""INSERT INTO warnings (device_id, warning_acceleration, warning_time, ingest_time) 
                            VALUES (%s, %s, %s, %s)
                        """)

peak_accel_table_insert = ("""INSERT INTO peak_accel (device_id, peak_acceleration, acceleration_time, ingest_time) 
                            VALUES (%s, %s, %s, %s)
                        """)

 
# QUERY LISTS

create_table_queries = [warning_table_create, peak_accel_table_create]
drop_table_queries = [warning_table_drop, peak_accel_table_drop]