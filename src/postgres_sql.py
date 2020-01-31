# DROP TABLES

warning_table_drop = "DROP table IF EXISTS warnings"
peak_accel_table_drop = "DROP table IF EXISTS peak_accel"


## CREATE TABLES

warning_table_create = ("""CREATE TABLE IF NOT EXISTS warnings (
                                device_id 				VARCHAR(5) NOT NULL, 
                                warning_acceleration 	NUMERIC,
                                warning_time			TIMESTAMP,
								id 						SERIAL PRIMARY KEY
                    );""")

peak_accel_table_create = ("""CREATE TABLE IF NOT EXISTS peak_accel ( 
                                device_id 				VARCHAR(5) NOT NULL, 
								count_acceleration    	INTEGER,
                                peak_acceleration    	NUMERIC,
                                acceleration_time		TIMESTAMP,                                
                                id 						SERIAL PRIMARY KEY
                    );""")


## INSERT RECORDS

warning_table_insert = ("""INSERT INTO warnings (device_id, warning_acceleration, warning_time) 
                            VALUES (%s, %s, %s)
                        """)

peak_accel_table_insert = ("""INSERT INTO peak_accel (device_id, peak_acceleration, acceleration_time) 
                            VALUES (%s, %s, %s)
                        """)

 
# QUERY LISTS

create_table_queries = [warning_table_create, peak_accel_table_create]
drop_table_queries = [warning_table_drop, peak_accel_table_drop]