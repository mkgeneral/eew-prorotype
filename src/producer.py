import sys
import os
import json
import logging
from time import sleep
from datetime import datetime, timedelta
import pandas as pd
from openeew.data.aws import AwsDataClient
from openeew.data.df import get_df_from_records
import boto3


def get_raw_data(data_client : AwsDataClient, start_date_utc : str, end_date_utc : str, docker_device_id = None):
    print(datetime.now())
    device_id = [docker_device_id]

    records_df_per_device = get_df_from_records(
        data_client.get_filtered_records(
            start_date_utc,
            end_date_utc,
            device_id ##TODO check if [None] and None would work
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
    print(datetime.now())
    return records_df


def main():
    # get country (= kafka topic) and device id (= kafka key) from input args
    if len(sys.argv) != 3:
        print('Please provide 2 arguments = country and device id')
    else:
        country = sys.argv[1]
        docker_device_id = sys.argv[2]
        print('inputs are {}, {}'.format( country, docker_device_id))
#    docker_device_id = os.environ['DEVICE']
    print('device_id for this run is {}'.format(docker_device_id))

    logging.basicConfig(level=logging.ERROR)
    data_client = AwsDataClient(country)

    producer = boto3.client('kinesis', region_name = 'us-west-2')
    # Asynchronous by default

    interval = timedelta(seconds=30)
    start_date = datetime(2020, 1, 5, 4, 39, 0)

    try:
        while True:
            start_date_utc = str(start_date)
            end_date = start_date + interval
            end_date_utc = str(end_date)
            accelerator_data = get_raw_data(data_client, start_date_utc, end_date_utc, docker_device_id)
            sleep_time = 1 / 64

            if accelerator_data is not None and accelerator_data.size > 0:
                print(f'Start_time: {start_date_utc}, Number of records: {str(accelerator_data.size)}')

                for reading in accelerator_data.itertuples():
                    sleep(sleep_time)

                    device_id = str(reading.device_id)
                    data = dict(reading._asdict()) #convert named tuple to dict

                    # produce asynchronously with callbacks
                    producer.put_record(StreamName='InputReadings',
                                        Data=json.dumps(data),
                                        PartitionKey=device_id)
            start_date = end_date

        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

    print('all done')

if __name__ == "__main__":
    main()