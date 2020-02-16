-- First in-app stream and pump
CREATE OR REPLACE STREAM "IN_APP_STREAM_001" (
   ingest_time   TIMESTAMP,
   device_id	 VARCHAR(5),
   acceleration	 DOUBLE,
   sample_tt     TIMESTAMP);
 
CREATE OR REPLACE PUMP "STREAM_PUMP_001" AS INSERT INTO "IN_APP_STREAM_001"
	SELECT STREAM  APPROXIMATE_ARRIVAL_TIME, 
	                "device_id", 
	                SQRT("y" * "y" +"z" * "z") AS acceleration, 
	                TO_TIMESTAMP(cast("sample_t"*1000 AS BIGINT))
		FROM "SOURCE_SQL_STREAM_001";

CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM_001" (
	device_id 			VARCHAR(5), 
	count_accel         INTEGER,
	peak_acceleration 	DOUBLE,
	acceleration_time	TIMESTAMP
);

-- first destination stream = peak acceleration per second (max of 32 accelerations)	
CREATE OR REPLACE PUMP "STREAM_PUMP_002" AS INSERT INTO "DESTINATION_SQL_STREAM_001"
    SELECT STREAM 	device_id, 
                COUNT(acceleration),
				MAX(acceleration) AS peak_acceleration,
				FLOOR(sample_tt TO SECOND) AS acceleration_time
				-- ingest_time
	FROM "IN_APP_STREAM_001"
    WINDOWED BY STAGGER (
            PARTITION BY FLOOR(sample_tt TO SECOND), device_id RANGE INTERVAL '2' SECOND);



CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM_002" (
	device_id 				VARCHAR(5), 
	warning_acceleration 	DOUBLE,
	warning_time			TIMESTAMP
	);

-- second destination stream = min peak acceleration for the past 3 seconds 
-- send WARNING if peak accelerations stayed above 0.5 for 3 seconds	
CREATE OR REPLACE PUMP "STREAM_PUMP_003" AS INSERT INTO "DESTINATION_SQL_STREAM_002"
    SELECT STREAM 	device_id, warning_acceleration, acceleration_time--, ingest_time
    FROM (
        SELECT STREAM 
                device_id,
				MIN(peak_acceleration) OVER ROW_SLIDING_WINDOW AS warning_acceleration,
				acceleration_time
	    FROM "DESTINATION_SQL_STREAM_001"
	    WINDOW ROW_SLIDING_WINDOW AS (PARTITION BY device_id ROWS 3 PRECEDING)
	)
	WHERE warning_acceleration >= 0.5;
