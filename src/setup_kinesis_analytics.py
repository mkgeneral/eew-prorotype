import boto3
import time
import logging
import sys
from configparser import ConfigParser

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

config = ConfigParser()
config.read_file(open('kinesis.cfg'))

APPNAME = config.get('KINESIS', 'app_name')
AWS_KINESIS_ROLE = config.get('KINESIS', 'aws_kinesis_role')

INPUT_STREAM = config.get('KINESIS', 'input_stream')
OUTPUT_ACCEL_STREAM = config.get('KINESIS', 'output_accel_stream')
OUTPUT_WARNING_STREAM = config.get('KINESIS', 'output_warning_stream')

INPUT_STREAM_SHARDS = config.get('KINESIS', 'input_stream_shards')
OUTPUT_ACCEL_STREAM_SHARDS = config.get('KINESIS', 'output_accel_stream_shards')
OUTPUT_WARNING_STREAM_SHARDS = config.get('KINESIS', 'output_warning_stream_shards')


class AwsSetup:
    def __init__(self):
        self._iam_client = boto3.client('iam')
        self._kinesis_client = boto3.client('kinesis')
        self._kinesisanalytics_client = boto3.client('kinesisanalytics')

        self._roleArn = self._create_role(AWS_KINESIS_ROLE)
        self._input_stream_arn = self._create_stream(INPUT_STREAM, INPUT_STREAM_SHARDS)
        self._output_accel_arn = self._create_stream(OUTPUT_ACCEL_STREAM, OUTPUT_ACCEL_STREAM_SHARDS)
        self._output_warning_arn = self._create_stream(OUTPUT_WARNING_STREAM, OUTPUT_WARNING_STREAM_SHARDS)

    def _create_role(self, role_name: str) -> str:
        """ create IAM role for Kinesis Analytics using predefined role"""

        logging.info(f'Creating IAM role for Kinesis Analytics ... ')
        if not AwsSetup._role_exsits(role_name):
            self._iam_client.create_role(RoleName=role_name,
                                                     AssumeRolePolicyDocument=TRUST_POLICY)
            self._iam_client.attach_role_policy(RoleName=role_name,
                                                PolicyArn="arn:aws:iam::aws:policy/AmazonKinesisAnalyticsFullAccess")

        logging.info(f'IAM role for Kinesis Analytics has been created')
        return self._iam_client.get_role(RoleName=role_name).get('Role').get('Arn')

    def _create_stream(self, stream_name: str, shards: int) -> str:
        """create kinesis stream for kinesis analytics, either input or output """

        logging.info(f'Creating Kinesis stream {stream_name} ... ')

        if not AwsSetup._stream_exists(stream_name):
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
            ApplicationName=APPNAME,
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
        while self._kinesisanalytics_client.describe_application(ApplicationName=APPNAME). \
                get('ApplicationDetail').get('ApplicationStatus') != 'READY':
            time.sleep(2)
        logging.info(f'Kinesis Analytics application {APPNAME} has been created, status = READY')

    @staticmethod
    def start_application():
        logging.info(f'Starting Kinesis Analytics application ... ')
        kinesisanalytics_client = boto3.client('kinesisanalytics')
        app_desc = kinesisanalytics_client.describe_application(ApplicationName=APPNAME). \
            get('ApplicationDetail')

        kinesisanalytics_client.start_application(ApplicationName=APPNAME,
                                                  InputConfigurations=[
                                                      {
                                                          'Id': app_desc.get('InputDescriptions')[0].get('InputId'),
                                                          'InputStartingPositionConfiguration': {
                                                              'InputStartingPosition': 'NOW'
                                                              # |'TRIM_HORIZON'|'LAST_STOPPED_POINT'
                                                          }
                                                      },
                                                  ])
        # wait until application has RUNNING status
        while kinesisanalytics_client.describe_application(ApplicationName=APPNAME). \
                get('ApplicationDetail').get('ApplicationStatus') != 'RUNNING':
            time.sleep(2)
        logging.info(f'Kinesis Analytics application {APPNAME} has been started')

    @staticmethod
    def stop_application():
        logging.info(f'Stopping Kinesis Analytics application ... ')
        kinesisanalytics_client = boto3.client('kinesisanalytics')
        app_desc = kinesisanalytics_client.describe_application(ApplicationName=APPNAME). \
            get('ApplicationDetail')

        if app_desc.get('ApplicationStatus') == 'RUNNING':
            kinesisanalytics_client.stop_application(ApplicationName=APPNAME)

        # wait until application has RUNNING status
        while kinesisanalytics_client.describe_application(ApplicationName=APPNAME). \
                get('ApplicationDetail').get('ApplicationStatus') != 'READY':
            time.sleep(2)
        logging.info(f'Kinesis Analytics application {APPNAME} has been stopped')

    @staticmethod
    def _stream_exists(stream_name: str) -> bool:
        kinesis_client = boto3.client('kinesis')
        streams = kinesis_client.list_streams()
        if stream_name in streams.get('StreamNames'):
            return True
        else:
            return False

    @staticmethod
    def _role_exsits(role_name: str) -> bool:
        iam_client = boto3.client('iam')
        roles_dict = iam_client.list_roles()
        roles_list = roles_dict.get('Roles')
        role_names = [role.get('RoleName') for role in roles_list]
        if role_name in role_names:
            return True
        else:
            return False

    @staticmethod
    def _application_exists(app_name: str) -> bool:
        kinesisanalytics_client = boto3.client('kinesisanalytics')
        apps_dict = kinesisanalytics_client.list_applications()
        apps_list = apps_dict.get('ApplicationSummaries')
        app_names = [app.get('ApplicationName') for app in apps_list]
        if app_name in app_names:
            return True
        else:
            return False

    @staticmethod
    def delete_streams():
        logging.info(f'Deleting input and output streams ... ')
        kinesis_client = boto3.client('kinesis')
        if AwsSetup._stream_exists(INPUT_STREAM):
            kinesis_client.delete_stream(StreamName=INPUT_STREAM)
        if AwsSetup._stream_exists(OUTPUT_ACCEL_STREAM):
            kinesis_client.delete_stream(StreamName=OUTPUT_ACCEL_STREAM)
        if AwsSetup._stream_exists(OUTPUT_WARNING_STREAM):
            kinesis_client.delete_stream(StreamName=OUTPUT_WARNING_STREAM)

    @staticmethod
    def delete_role():
        logging.info(f'Deleting IAM role for Kinesis Analytics ... ')
        iam_client = boto3.client('iam')
        if AwsSetup._role_exsits(AWS_KINESIS_ROLE):
            iam_client.detach_role_policy(RoleName=AWS_KINESIS_ROLE,
                                          PolicyArn="arn:aws:iam::aws:policy/AmazonKinesisAnalyticsFullAccess")
            iam_client.delete_role(RoleName=AWS_KINESIS_ROLE)

    @staticmethod
    def delete_application():
        logging.info(f'Deleting Kinesis Analytics application ... ')
        kinesisanalytics_client = boto3.client('kinesisanalytics')

        if AwsSetup._application_exists(APPNAME):
            app_desc = kinesisanalytics_client.describe_application(ApplicationName=APPNAME). \
                get('ApplicationDetail')

            kinesisanalytics_client.delete_application(ApplicationName=APPNAME,
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
            # create role, streams and application
            aws = AwsSetup()
            aws.create_application()
        elif action == 'start':
            AwsSetup.start_application()
        elif action == 'stop':
            AwsSetup.stop_application()
        elif action == 'delete':
            # delete application, streams and role
            AwsSetup.delete_application()
        else:
            print('Action has not been recognized, please enter create, start, stop or delete')
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
