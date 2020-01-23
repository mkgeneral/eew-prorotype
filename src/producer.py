from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
import os
import json
import logging
from time import sleep
from datetime import datetime
import pandas as pd
from openeew.data.aws import AwsDataClient
from openeew.data.df import get_df_from_records

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
    docker_device_id = os.environ['DEVICE']
    print('device_id for this run is {}'.format(docker_device_id))

    logging.basicConfig(level=logging.ERROR)
    data_client = AwsDataClient('mx')

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], #['127.0.0.1:9090'], #
                            acks = 0,
                            key_serializer = lambda m: m.encode('utf-8'),
                            value_serializer = lambda m: json.dumps(m).encode('utf-8'))

    def on_send_success(record_metadata):
        print ('topic = {}'.format(record_metadata.topic))
        print ('partition = {}'.format(record_metadata.partition))
        print ('offset = {}'.format(record_metadata.offset))

    def on_send_error(excp):
        print('I am an errback' + str(excp))
        # handle exception

    # Asynchronous by default
    try:
        start_date_utc = '2020-01-05 04:41:00'
        end_date_utc = '2020-01-05 04:41:15'
        accelerator_data = get_raw_data(data_client, start_date_utc, end_date_utc, docker_device_id)
        sleep_time = 1 / 64

        if accelerator_data is not None and accelerator_data.size > 0:
            print(accelerator_data.size)

            for reading in accelerator_data.itertuples():
                sleep(sleep_time)

                device_id = str(reading.device_id)

                # produce asynchronously with callbacks
                producer.send(country, key=device_id, value=reading).add_callback(on_send_success).add_errback(on_send_error)

        producer.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

    if producer is not None:
        producer.close()
        print('closed')

    print('all done')

if __name__ == "__main__":
    main()