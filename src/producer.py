from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging
from time import sleep
from datetime import datetime
import pandas as pd
from openeew.data.aws import AwsDataClient
from openeew.data.df import get_df_from_records

country = 'mx'
docker_device_id = ['005']
kafka_topic = country

def get_raw_data(data_client):
    print(datetime.now())
    start_date_utc = '2020-01-05 04:41:00'
    end_date_utc = '2020-01-05 04:42:00'
    device_id = docker_device_id

    records_df_per_device = get_df_from_records(
        data_client.get_filtered_records(
            start_date_utc,
            end_date_utc,
            #device_id
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
    logging.basicConfig(level=logging.ERROR)
    data_client = AwsDataClient('mx')

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
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
        links = get_raw_data(data_client)
        if links is not None and links.size > 0:
            print(links.size)

            for link in links.itertuples():
                # sleep(2)

                _id = str(link.device_id)
                result = {'device': link.device_id, 'x': link.x, 'y': link.y, 'z': link.z, 't': link.sample_t}

                # produce asynchronously with callbacks
                producer.send(kafka_topic, key=_id, value=result).add_callback(on_send_success).add_errback(on_send_error)

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