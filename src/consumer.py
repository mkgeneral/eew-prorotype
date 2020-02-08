import os
import boto3
import json
import logging
from datetime import datetime, timedelta
from configparser import ConfigParser
import psycopg2
import psycopg2.extensions
import psycopg2.extras
from openeew.data.aws import AwsDataClient

# from collections import namedtuple
# from postgres_sql import *

config = ConfigParser()
config.read_file(open('postgres.cfg'))

DBHOST = config.get('POSTGRES', 'dbhost')
DBNAME = config.get('POSTGRES', 'dbname')
DBUSER = config.get('POSTGRES', 'dbuser')
DBPASSWORD = config.get('POSTGRES', 'dbpassword')

SUBSCRIBERS = config.get('SNS_SUBSCRIBERS', 'numbers')


class AbstractStream:
    def __init__(self, name: str, postgres_page_size: int = 1000):
        self._stream_name = name
        self._stream_table_name = ''
        self._postgres_page_size = postgres_page_size

    @property
    def stream_name(self) -> str:
        return self._stream_name

    def setup_tbl(self, cur: psycopg2.extensions.cursor):
        pass

    def save_to_postgres(self, cur: psycopg2.extensions.cursor, records: list):
        """save data from a Kinesis stream to a postgres table"""

        record_iterator = (tuple(json.loads(record["Data"]).values()) for record in records)
        psycopg2.extras.execute_values(cur,  # db cursor
                                       f"INSERT INTO {self._stream_table_name} VALUES %s;",
                                       record_iterator,  # iterator
                                       page_size=self._postgres_page_size)


class OutputAccelerationStream(AbstractStream):
    def __init__(self, name: str, postgres_page_size: int = 1000):

        super(OutputAccelerationStream, self).__init__(name, postgres_page_size)
        self._stream_table_name = 'peak_accel'

    def setup_tbl(self, cur: psycopg2.extensions.cursor):
        """ setup destination tables in PostgreSQL"""
        try:
            cur.execute(f"DROP table IF EXISTS {self._stream_table_name}")
            cur.execute(f"""CREATE TABLE IF NOT EXISTS {self._stream_table_name} ( 
                                device_id 				VARCHAR(5) NOT NULL, 
                                count_acceleration    	INTEGER,
                                peak_acceleration    	NUMERIC,
                                acceleration_time		TIMESTAMP,                                
                                id 						SERIAL PRIMARY KEY
                          );""")
        except psycopg2.Error as e:
            print(e)


class OutputWarningStream(AbstractStream):
    def __init__(self, name: str, postgres_page_size: int = 1000):
        super(OutputWarningStream, self).__init__(name, postgres_page_size)
        self._stream_table_name = 'warnings'

    def setup_tbl(self, cur: psycopg2.extensions.cursor):
        """ setup destination tables in PostgreSQL"""
        try:
            cur.execute(f"DROP table IF EXISTS {self._stream_table_name}")
            cur.execute(f"""CREATE TABLE IF NOT EXISTS {self._stream_table_name} ( 
                            device_id 				VARCHAR(5) NOT NULL, 
                            warning_acceleration 	NUMERIC,
                            warning_time			TIMESTAMP,
                            id 						SERIAL PRIMARY KEY
                          );""")
        except psycopg2.Error as e:
            print(e)


class StreamFactory:

    @staticmethod
    def produce_stream(stream_name: str) -> AbstractStream:
        if  'OutputAccelerations' in stream_name:
            return OutputAccelerationStream(
                name=stream_name)
        else:
            return OutputWarningStream(name='OutputWarning')


class StreamConsumer:
    def __init__(self, stream_obj: AbstractStream,
                 kinesis_records_limit=1000):
        """ initialize Kinesis as consumer input,
            SNS as alert publishing system,
            PostgreSQL as output storage system
        """
        self._kinesis_records_limit = kinesis_records_limit
        self._stream_obj = stream_obj

        # set up PostgreSQL to save calculated accelerations
        # self._setup_db()  # run this only once
        self._connect_db()  # sets up db cursor
        stream_obj.setup_tbl(self._cur)

        # set up Kinesis to read output stream
        self._setup_kinesis()

        # setup notification system to publish alerts
        self._setup_sns()
        self._sent_sms = {}

    def _setup_db(self):
        """setup database in PostgreSQL"""
        conn = psycopg2.connect(f"host={DBHOST} user={DBUSER} password={DBPASSWORD}")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        cur.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
        cur.execute(f"CREATE DATABASE {DBNAME} WITH ENCODING 'utf8' TEMPLATE template0")
        conn.close()

    def _connect_db(self):
        """ connect to postgreSQL database"""
        conn = psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASSWORD}")
        conn.set_session(autocommit=True)
        self._cur = conn.cursor()

    def _setup_kinesis(self):
        """ setup Kinesis connection and shard iterator"""

        self._kinesis = boto3.client('kinesis')
        response = self._kinesis.describe_stream(StreamName=self._stream_obj.stream_name)

        shard_id = response['StreamDescription']['Shards'][0]['ShardId']
        shard_iterator = self._kinesis.get_shard_iterator(StreamName=self._stream_obj.stream_name,
                                                          ShardId=shard_id,
                                                          ShardIteratorType='LATEST')  # 'LATEST' or 'TRIM_HORIZON'
        self._shard_it = shard_iterator['ShardIterator']

    def _setup_sns(self):
        """ setup notification system, SNS"""
        self._sns_client = boto3.client("sns")

        # Create the topic if it doesn't exist (this is idempotent)
        topic = self._sns_client.create_topic(Name="notifications")
        self._topic_arn = topic['TopicArn']  # get its Amazon Resource Name

        numbers = SUBSCRIBERS.split(',')
        # Add SMS Subscribers
        for number in numbers:
            self._sns_client.subscribe(
                TopicArn=self._topic_arn,
                Protocol='sms',
                Endpoint=number  # <-- number who'll receive an SMS message.
            )

    def _get_device_location(self, device_id: str, alert_date: str, country: str = 'mx') -> dict:
        data_client = AwsDataClient(country)

        devices = data_client.get_devices_as_of_date(alert_date)
        location = {}
        for device in devices:
            if device['device_id'] == device_id:
                location = {'latitude': device['latitude'], 'longitude': device['longitude']}
        return location

    def _send_sms(self, records: list) -> bool:
        """ Send a message when new records show up in warnings stream
            The closest device should send message first
            All other devices will have a time lag
        """
        if records and len(records):
            data = json.loads(records[0]["Data"])
            device_id = data.get('DEVICE_ID')

            if self._sent_sms.get(device_id) is None:
                location = self._get_device_location(device_id, data.get('WARNING_TIME')[:-4])
                message = f"Detected ground shaking above threshold {data.get('WARNING_ACCELERATION')}, " \
                          f"time {data.get('WARNING_TIME')}, " \
                          f"by device {device_id} at {str(location)}"

                # Publish a message.
                self._sns_client.publish(Message="Warning!", TopicArn=self._topic_arn)
                self._sns_client.publish(Message=message, TopicArn=self._topic_arn)

                self._sent_sms.update({data.get('DEVICE_ID'): data.get('WARNING_TIME')[:-4]})
            return True
        else:
            return False

    def consume_records(self) -> None:
        """read output stream from Kinesis after Kinesis Analytics processing"""

        # the limit is 5 reads from output stream per second = use 5 tokens per second to meter reads
        end_time = datetime.now() + timedelta(seconds=1)
        tokens = 5
        sent_message = False
        while True:
            try:
                while datetime.now() <= end_time:
                    if tokens > 0:
                        # read records from Kinesis
                        records = self._kinesis.get_records(ShardIterator=self._shard_it, Limit=self._kinesis_records_limit)
                        logging.info(str(records['ResponseMetadata']['HTTPHeaders']['date']) + ' ' + str(tokens) + ' ' + str(len(records["Records"])))
                        # logging.info(len(records["Records"]))

                        # save records into Postgres, each stream corresponds to a table
                        self._stream_obj.save_to_postgres(self._cur, records["Records"])
                        self._shard_it = records["NextShardIterator"]

                        # TODO define how to send message only for the first device that shows up in warning stream
                        if 'warning' in self._stream_obj.stream_name.lower():  # and not sent_message:
                            sent_message = self._send_sms(records["Records"])

                        tokens -= 1

                end_time = end_time + timedelta(seconds=1)
                tokens = 5


            except psycopg2.Error as e:
                logging.error(f'Error saving data in PostgreSQL: {e}')
            except Exception as generic:
                logging.error(f'Error while consuming data from Kinesis: {generic}')


def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s:  %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S')

    stream_name = os.environ['STREAM_NAME']
    logging.info('Stream_name is {}'.format(stream_name))

    output_stream = StreamFactory.produce_stream(stream_name)
    consumer = StreamConsumer(output_stream, kinesis_records_limit=10000)
    consumer.consume_records()


if __name__ == "__main__":
    main()
