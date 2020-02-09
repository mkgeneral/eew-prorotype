import boto3
import time
import logging
import sys

KINESIS_ANALYTICS_CODE = """
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

CREATE OR REPLACE STREAM "IN_APP_STREAM_002" (
   ingest_time   TIMESTAMP,
   shard_id VARCHAR(20),
   device_id	 VARCHAR(5),
   sample_tt     TIMESTAMP);
 
CREATE OR REPLACE PUMP "STREAM_PUMP_0012" AS INSERT INTO "IN_APP_STREAM_002"
    SELECT STREAM  APPROXIMATE_ARRIVAL_TIME, 
                SHARD_ID,
                "device_id", 
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
            PARTITION BY device_id, FLOOR(sample_tt TO SECOND) RANGE INTERVAL '1' SECOND);


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
"""


TRUST_POLICY = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "kinesisanalytics.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}"""


class AwsSetup:
    def __init__(self, app_name: str):
        self._iam_client = boto3.client('iam')
        self._kinesis_client = boto3.client('kinesis')
        self._kinesisanalytics_client = boto3.client('kinesisanalytics')

        self._roleArn = self._create_role('KinesisAnalyticsRole')
        self._application_name = app_name
        self._input_stream_arn = self._create_stream('InputReadings-cli', 2)
        self._output_accel_arn = self._create_stream('OutputAccelerations-cli', 1)
        self._output_warning_arn = self._create_stream('OutputWarning-cli', 1)

    def _create_role(self, role_name: str) -> str:
        """ create IAM role for Kinesis Analytics using predefined role"""

        logging.info(f'Creating IAM role for Kinesis Analytics ... ')
        role_desc = self._iam_client.create_role(RoleName=role_name,
                                                 AssumeRolePolicyDocument=TRUST_POLICY)
        self._iam_client.attach_role_policy(RoleName=role_name,
                                            PolicyArn="arn:aws:iam::aws:policy/AmazonKinesisAnalyticsFullAccess")
        logging.info(f'IAM role for Kinesis Analytics has been created')
        return role_desc.get('Role').get('Arn')

    def _create_stream(self, stream_name: str, shards: int) -> str:
        """create kinesis stream for kinesis analytics, either input or output """

        logging.info(f'Creating Kinesis stream {stream_name} ... ')
        self._kinesis_client.create_stream(StreamName=stream_name, ShardCount=shards)

        # wait until stream has Active status
        while self._kinesis_client.describe_stream_summary(StreamName=stream_name). \
            get('StreamDescriptionSummary').get('StreamStatus') != 'ACTIVE':
            time.sleep(2)
        logging.info(f'Stream {stream_name} has been activated')

        return self._kinesis_client.describe_stream_summary(StreamName=stream_name). \
            get('StreamDescriptionSummary').get('StreamARN')

    def create_application(self):
        logging.info(f'Creating Kinesis Analytics application ... ')

        self._kinesisanalytics_client.create_application(
            ApplicationName=self._application_name,
            ApplicationDescription='EarthquakeEarlyWarning-cli',
            Inputs=[
                {
                    'NamePrefix': 'SOURCE_SQL_STREAM',
                    'KinesisStreamsInput': {
                        'ResourceARN': self._input_stream_arn,
                        'RoleARN': self._roleArn
                    },
                    'InputParallelism': {
                        'Count': 1
                    },
                    'InputSchema': {
                        "RecordColumns": [
                            {
                                "Name": "Index",
                                "SqlType": "SMALLINT",
                                "Mapping": "$.Index"
                            },
                            {
                                "Name": "device_id",
                                "SqlType": "VARCHAR(4)",
                                "Mapping": "$.device_id"
                            },
                            {
                                "Name": "x",
                                "SqlType": "DOUBLE",
                                "Mapping": "$.x"
                            },
                            {
                                "Name": "y",
                                "SqlType": "DOUBLE",
                                "Mapping": "$.y"
                            },
                            {
                                "Name": "z",
                                "SqlType": "DOUBLE",
                                "Mapping": "$.z"
                            },
                            {
                                "Name": "sample_t",
                                "SqlType": "DOUBLE",
                                "Mapping": "$.sample_t"
                            }
                        ],
                        "RecordEncoding": "UTF-8",
                        "RecordFormat": {
                            "RecordFormatType": "JSON",
                            "MappingParameters": {
                                "JSONMappingParameters": {
                                    "RecordRowPath": "$"
                                }
                            }
                        }
                    }
                }
            ],
            Outputs=[
                {
                    'Name': 'DESTINATION_SQL_STREAM_001',
                    'KinesisStreamsOutput': {
                        'ResourceARN': self._output_accel_arn,
                        'RoleARN': self._roleArn
                    },
                    'DestinationSchema': {
                        'RecordFormatType': 'JSON'
                    }
                },
                {
                    'Name': 'DESTINATION_SQL_STREAM_002',
                    'KinesisStreamsOutput': {
                        'ResourceARN': self._output_warning_arn,
                        'RoleARN': self._roleArn
                    },
                    'DestinationSchema': {
                        'RecordFormatType': 'JSON'
                    }
                },
            ],
            ApplicationCode=KINESIS_ANALYTICS_CODE,
        )

        # wait until application has Ready status
        while self._kinesisanalytics_client.describe_application(ApplicationName=self._application_name). \
            get('ApplicationDetail').get('ApplicationStatus') != 'READY':
            time.sleep(2)
        logging.info(f'Kinesis Analytics application {self._application_name} has been created, status = READY')

    @staticmethod
    def start_application(application_name):
        logging.info(f'Starting Kinesis Analytics application ... ')
        kinesisanalytics_client = boto3.client('kinesisanalytics')
        app_desc = kinesisanalytics_client.describe_application(ApplicationName=application_name). \
            get('ApplicationDetail')

        kinesisanalytics_client.start_application(ApplicationName=application_name,
                                                  InputConfigurations=[
                                                      {
                                                          'Id': app_desc.get('InputDescriptions').get('InputId'),
                                                          'InputStartingPositionConfiguration': {
                                                              'InputStartingPosition': 'NOW'
                                                              # |'TRIM_HORIZON'|'LAST_STOPPED_POINT'
                                                          }
                                                      },
                                                  ])
        # wait until application has RUNNING status
        while kinesisanalytics_client.describe_application(ApplicationName=application_name). \
            get('ApplicationDetail').get('ApplicationStatus') != 'RUNNING':
            time.sleep(2)
        logging.info(f'Kinesis Analytics application {application_name} has been started')

    @staticmethod
    def stop_application(application_name):
        logging.info(f'Stopping Kinesis Analytics application ... ')
        kinesisanalytics_client = boto3.client('kinesisanalytics')
        app_desc = kinesisanalytics_client.describe_application(ApplicationName=application_name). \
            get('ApplicationDetail')

        if app_desc.get('ApplicationStatus') == 'RUNNING':
            kinesisanalytics_client.stop_application(ApplicationName=application_name)

        # wait until application has RUNNING status
        while kinesisanalytics_client.describe_application(ApplicationName=application_name). \
            get('ApplicationDetail').get('ApplicationStatus') != 'READY':
            time.sleep(2)
        logging.info(f'Kinesis Analytics application {application_name} has been stopped')

    @staticmethod
    def delete_streams():
        pass

    @staticmethod
    def delete_role():
        pass

    @staticmethod
    def delete_application(application_name):
        logging.info(f'Deleting Kinesis Analytics application ... ')
        kinesisanalytics_client = boto3.client('kinesisanalytics')
        app_desc = kinesisanalytics_client.describe_application(ApplicationName=application_name). \
            get('ApplicationDetail')

        kinesisanalytics_client.delete_application(ApplicationName=application_name,
                                                   CreateTimestamp=app_desc.get('CreateTimestamp'))
        AwsSetup.delete_streams()
        AwsSetup.delete_role()


def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s:  %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S')

    if len(sys.argv) != 2:
        print('Please provide 1 argument = action: create, start, stop, delete')
        exit()

    action = sys.argv[1]

    try:
        if action == 'create':
            # creates application, streams and role
            aws = AwsSetup('eew-cli')
            aws.create_application()
        elif action == 'start':
            AwsSetup.start_application('eew-cli')
        elif action == 'stop':
            AwsSetup.stop_application('eew-cli')
        elif action == 'delete':
            # deletes application, streams and role
            AwsSetup.delete_application('eew-cli')
        else:
            print('Action has not been recognized, please enter create, start, stop or delete')
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
