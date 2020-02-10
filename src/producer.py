import sys
import os
import json
import logging
import pandas as pd
from time import sleep
from datetime import datetime, timedelta
from openeew.data.aws import AwsDataClient
from openeew.data.df import get_df_from_records
import boto3
from configparser import ConfigParser

config = ConfigParser()
config.read_file(open('kinesis.cfg'))
INPUT_STREAM = config.get('KINESIS', 'input_stream')


class StreamProducer:
    def __init__(self,
                 docker_device_id,
                 country='mx',
                 stream_name='InputReadings',
                 start_date=datetime(2020, 1, 5, 4, 39, 0),
                 end_date=datetime(2020, 2, 5, 4, 40, 0),
                 kinesis_produce_many=False,
                 kinesis_batch_size=20,
                 reading_frequency=64,  # sensor readings per second
                 reading_interval=30):  # reading raw data from S3
        """ initialize Kinesis as consumer input,
            SNS as alert publishing system,
            PostgreSQL as output storage system
        """
        self._docker_device_id = docker_device_id
        self._stream_name = stream_name

        self._start_date = start_date
        self._end_date = end_date
        self._sleep_time = 1 / reading_frequency if reading_frequency > 0 else 1/32
        self._interval = timedelta(seconds=reading_interval)

        self._kinesis_produce_many = kinesis_produce_many
        self._kinesis_batch_size = kinesis_batch_size
        self._kinesis = boto3.client('kinesis')  # , region_name='us-west-2')
        self._data_client = AwsDataClient(country)

    def _get_raw_data(self,
                      start_date: datetime,
                      end_date: datetime,
                      device_id=None) -> pd.DataFrame:
        """get sensor readings from S3 using OpenEEW API"""

        records_df_per_device = get_df_from_records(
            self._data_client.get_filtered_records(
                str(start_date),  # utc date
                str(end_date),    # utc date
                [device_id]       # list of devices
            )
        )
        # Select required columns
        records_df = records_df_per_device[
            [
                'device_id',
                'x',
                'y',
                'z',
                'sample_t'
            ]
        ]
        return records_df

    def _send_data_to_kinesis(self, accelerator_data: pd.DataFrame):
        """ send raw data for 1 time interval to Kinesis """
        i = 0
        records = []

        if accelerator_data is not None and accelerator_data.size > 0:
            num_readings = accelerator_data.size
            # logging.info(f'Start_time: {str(start_date)}, Number of records: {str(num_readings)}')
            for reading in accelerator_data.itertuples():
                sleep(self._sleep_time)

                device_id = str(reading.device_id)
                data = dict(reading._asdict())  # convert named tuple to dict

                if self._kinesis_produce_many:
                    # create a set of records to be pushed together
                    i += 1
                    record = {'Data': json.dumps(data), 'PartitionKey': str(reading.device_id)}
                    records.append(record)

                    if i % self._kinesis_batch_size == 0 or i == num_readings:
                        self._kinesis.put_records(StreamName=self._stream_name, Records=records)
                        records = []
                else:
                    self._kinesis.put_record(StreamName=self._stream_name,
                                             Data=json.dumps(data),
                                             PartitionKey=str(reading.device_id))

    def produce(self):
        """send sensor data to input Kinesis stream"""

        start_date_time = self._start_date
        end_date_time = self._start_date

        while end_date_time < self._end_date:  # True
            try:
                end_date_time = start_date_time + self._interval
                sensor_data = self._get_raw_data(start_date_time, end_date_time, self._docker_device_id)
                self._send_data_to_kinesis(sensor_data)

                # move onto the next time interval
                start_date_time = end_date_time
            except Exception as ex:
                print('Exception in publishing message')
                print(str(ex))


def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s:  %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S')

    if len(sys.argv) != 3:
        print('Please provide 2 arguments = country and device id')
        exit()

    country = sys.argv[1]
    docker_device_id = os.environ['DEVICE']
    if not docker_device_id:
        docker_device_id = sys.argv[2]
    logging.info('inputs are {}, {}'.format(country, docker_device_id))

    # send data to message broker
    producer = StreamProducer(docker_device_id, stream_name=INPUT_STREAM)
    producer.produce()


if __name__ == "__main__":
    main()
