import boto3
import json
import psycopg2
from src.postgres_sql import *
# from datetime import datetime
# import time

dbhost = 'postgresql.cmp8s1zeudhi.us-west-2.rds.amazonaws.com'
dbname = 'eewdb'
dbuser = 'postgres'
dbpassword = 'oIDED14PpCSHHjjdg6sh'

def consume_records(cur):
    try:
        setup_tbl(cur)
    except psycopg2.Error as e:
        print(e)

    stream_name = 'OutputWarning'

    kinesis = boto3.client('kinesis')
    response = kinesis.describe_stream(StreamName=stream_name)

    shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = kinesis.get_shard_iterator(StreamName=stream_name,
                                              ShardId=shard_id,
                                              ShardIteratorType='LATEST') #'LATEST') #'TRIM_HORIZON')
    shard_it = shard_iterator['ShardIterator']

    while True:
        records = kinesis.get_records(ShardIterator=shard_it,
                                      Limit=2)
        print(records)
        for record in records["Records"]:
            data = json.loads(record["Data"])
            print(data)

            try:
                cur.execute(peak_accel_table_insert, tuple(data.values())) #('005', 1.2, '2020-05-05 20:20:20', '2020-05-05 20:20:20'))
            except psycopg2.Error as e:
                print(e)

        shard_it = records["NextShardIterator"]

        # sum = sum + data["age"]
        # i = i + 1

        # wait for 5 seconds
        # time.sleep(5)


def setup_db():
    conn = psycopg2.connect(
        f"host={dbhost} user={dbuser} password={dbpassword}")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS {dbname}")
    cur.execute(f"CREATE DATABASE {dbname} WITH ENCODING 'utf8' TEMPLATE template0")
    conn.close()


def setup_tbl(cur):
    cur.execute(peak_accel_table_drop)
    cur.execute(peak_accel_table_create)


def main():
    # setup Postgres db and table
    setup_db()

    conn = psycopg2.connect(f"host={dbhost} dbname={dbname} user={dbuser} password={dbpassword}")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    try:
        setup_tbl(cur)
        # cur.execute(peak_accel_table_insert, ('005', 1.2, '2020-05-05 20:20:20', '2020-05-05 20:20:20'))
    except psycopg2.Error as e:
        print(e)

    consume_records(cur)


if __name__ == "__main__":
    main()