import os
from typing import Dict

import boto3
import json
from collections import namedtuple
from configparser import ConfigParser
import psycopg2
import psycopg2.extras
from postgres_sql import *

config = ConfigParser()
config.read_file(open('postgres.cfg'))

DBHOST = config.get('POSTGRES', 'dbhost')
DBNAME = config.get('POSTGRES', 'dbname')
DBUSER = config.get('POSTGRES', 'dbuser')
DBPASSWORD = config.get('POSTGRES', 'dbpassword')

StreamDef = namedtuple('StreamDef', 'table, drop_query, create_query')
accel_stream = StreamDef(table='peak_accel',
                         drop_query=peak_accel_table_drop,
                         create_query=peak_accel_table_create)
warning_stream = StreamDef(table='warnings',
                           drop_query=warning_table_drop,
                           create_query=warning_table_create)

STREAM_TO_TABLE = {'OutputAccelerations': accel_stream,
                   'OutputWarning': warning_stream}

COPY_MANY = True
POSTGRES_PAGE_SIZE = 1000


def setup_db():
    """setup database in PostgreSQL"""
    conn = psycopg2.connect(
        f"host={DBHOST} user={DBUSER} password={DBPASSWORD}")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
    cur.execute(f"CREATE DATABASE {DBNAME} WITH ENCODING 'utf8' TEMPLATE template0")
    conn.close()


def setup_tbl(cur: psycopg2.extensions.cursor, stream_def: namedtuple) -> None:
    """ setup destination tables in PostgreSQL"""
    try:
        cur.execute(stream_def.drop_query)
        cur.execute(stream_def.create_query)
    except psycopg2.Error as e:
        print(e)


def setup_kinesis(stream_name: str) -> list:
    """ setup Kinesis connection and shard iterator"""

    kinesis = boto3.client('kinesis')
    response = kinesis.describe_stream(StreamName=stream_name)

    shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = kinesis.get_shard_iterator(StreamName=stream_name,
                                                ShardId=shard_id,
                                                ShardIteratorType='LATEST')  # 'LATEST' or 'TRIM_HORIZON'
    shard_it = shard_iterator['ShardIterator']
    return [kinesis, shard_it]


def consume_records(cur: psycopg2.extensions.cursor,
                    stream_name: str) -> None:
    """read output stream from Kinesis after Kinesis Analytics processing"""
    stream_def = STREAM_TO_TABLE.get(stream_name)
    setup_tbl(cur, stream_def)

    kinesis, shard_it = setup_kinesis(stream_name)

    while True:
        # read records from Kinesis
        try:
            records = kinesis.get_records(ShardIterator=shard_it,
                                          Limit=1000)
        except Exception as e:
            print(e)

        print(len(records["Records"]))

        # save records in Postgres
        try:
            if COPY_MANY:
                psycopg2.extras.execute_values(
                    cur,
                    f"""INSERT INTO {stream_def.table} VALUES %s;""",
                    (tuple(json.loads(record["Data"]).values()) for record in records["Records"]),
                    page_size=POSTGRES_PAGE_SIZE)
            else:
                for record in records["Records"]:
                    data = json.loads(record["Data"])
                    print(data)
                    cur.execute(f"""INSERT INTO {stream_def.table} VALUES %s;""",
                                tuple(data.values())) #json.loads(record["Data"])
        except psycopg2.Error as e:
            print(e)

        shard_it = records["NextShardIterator"]


def main():
    stream_name = os.environ['STREAM_NAME']
    print('stream_name for this run is {}'.format(stream_name))

    # setup Postgres db and table, do it only once
    # setup_db()

    conn = psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASSWORD}")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    consume_records(cur, stream_name)


if __name__ == "__main__":
    main()
