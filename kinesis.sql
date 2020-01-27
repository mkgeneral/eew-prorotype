-- First in-app stream and pump
CREATE OR REPLACE STREAM "IN_APP_STREAM_001" (
   ingest_time   TIMESTAMP,
   device_id	 VARCHAR(5),
   acceleration	 DOUBLE,
   sample_t      DOUBLE);
 
CREATE OR REPLACE PUMP "STREAM_PUMP_001" AS INSERT INTO "IN_APP_STREAM_001"
	SELECT STREAM  APPROXIMATE_ARRIVAL_TIME, 
	                "device_id", 
	                SQRT("y" * "y" +"z" * "z") AS acceleration, 
	                "sample_t"
		FROM "SOURCE_SQL_STREAM_001";

CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM_001" (
	device_id 			VARCHAR(5), 
	peak_acceleration 	DOUBLE,
	acceleration_time	DOUBLE);

-- first destination stream = peak acceleration per second (max of 32 accelerations)	
CREATE OR REPLACE PUMP "STREAM_PUMP_002" AS INSERT INTO "DESTINATION_SQL_STREAM_001"
    SELECT STREAM 	device_id, 
				MAX(acceleration) OVER ROW_SLIDING_WINDOW AS peak_acceleration,
				MAX(sample_t) 	  OVER ROW_SLIDING_WINDOW AS acceleration_time
	FROM "IN_APP_STREAM_001"
	WINDOW ROW_SLIDING_WINDOW AS (PARTITION BY device_id ROWS 32 PRECEDING);

CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM_002" (
	device_id 				VARCHAR(5), 
	warning_acceleration 	DOUBLE,
	warning_time			TIMESTAMP--,
-- 	ingest_time   			TIMESTAMP
	);

-- second destination stream = min peak acceleration for the past 3 seconds 
-- send WARNING if peak accelerations stayed above 0.5 for 3 seconds	
CREATE OR REPLACE PUMP "STREAM_PUMP_003" AS INSERT INTO "DESTINATION_SQL_STREAM_002"
    SELECT STREAM 	device_id, warning_acceleration, TO_TIMESTAMP(cast(acceleration_time*1000 AS BIGINT))
    FROM (
        SELECT STREAM 
                device_id,
				MIN(peak_acceleration) OVER ROW_SLIDING_WINDOW AS warning_acceleration,
				acceleration_time--,
				-- ROWTIME
	    FROM "DESTINATION_SQL_STREAM_001"
	    WINDOW ROW_SLIDING_WINDOW AS (PARTITION BY device_id ROWS 3 PRECEDING)
	)
	WHERE warning_acceleration >= 0.5;
