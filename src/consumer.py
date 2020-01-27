import boto3
import json
import psycopg2
# from datetime import datetime
# import time

def consume_records():

    stream_name = 'OutputWarning'

    kinesis = boto3.client('kinesis')
    response = kinesis.describe_stream(StreamName=stream_name)

    shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = kinesis.get_shard_iterator(StreamName=stream_name,
                                              ShardId=shard_id,
                                              ShardIteratorType='LATEST') #'TRIM_HORIZON')
    shard_it = shard_iterator['ShardIterator']

    while True:
        records = kinesis.get_records(ShardIterator=shard_it,
                                      Limit=2)
        print(records)
        for record in records["Records"]:
            data = json.loads(record["Data"])
            print(data)

        shard_it = records["NextShardIterator"]

        # sum = sum + data["age"]
        # i = i + 1

        # wait for 5 seconds
        # time.sleep(5)

def main():
    # conn = psycopg2.connect("host=postgresql.cmp8s1zeudhi.us-west-2.rds.amazonaws.com user=postgres password=oIDED14PpCSHHjjdg6sh")
    # conn.set_session(autocommit=True)
    # cur = conn.cursor()
    # conn.close()
    #
    # cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    conn = psycopg2.connect("host=postgresql.cmp8s1zeudhi.us-west-2.rds.amazonaws.com user=postgres password=oIDED14PpCSHHjjdg6sh")
    cur = conn.cursor()

    user_table_drop = "DROP table IF EXISTS users"
    cur.execute(user_table_drop)
    conn.commit()
    user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id   int PRIMARY KEY, 
                                                             first_name varchar NOT NULL, 
                                                             last_name  varchar NOT NULL, 
                                                             gender     varchar, 
                                                             level      varchar);""")
    cur.execute(user_table_create)
    conn.commit()

    user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) 
                                VALUES ('1', 'abc', 'dfg', 'F', 'paid') 
                                ON CONFLICT (user_id)
                                DO
                                  UPDATE
                                  SET level = EXCLUDED.level""") #%s, %s, %s, %s, %s)

    cur.execute(user_table_insert) #, ['1', 'abc', 'dfg', 'F', 'paid'])
    results = cur.execute('select * from users')
    print(results)
    # consume_records()

if __name__ == "__main__":
    main()