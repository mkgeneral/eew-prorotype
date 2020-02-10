"""
this is an ancillary script to verify that data was loaded into PostgreSQL db
and to plot output ground accelerations
"""

import psycopg2
import logging
from configparser import ConfigParser
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2.extras

config = ConfigParser()
config.read_file(open('postgres.cfg'))

DBHOST = config.get('POSTGRES', 'dbhost')
DBNAME = config.get('POSTGRES', 'dbname')
DBUSER = config.get('POSTGRES', 'dbuser')
DBPASSWORD = config.get('POSTGRES', 'dbpassword')


def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s:  %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S')

    conn = psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASSWORD}")
    conn.set_session(autocommit=True)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    print(type(cur))
    # verify that data was loaded into PostgreSQL
    try:
        cur.execute('select count(*) from warnings')  # warnings or peak_accel
        for record in cur:
            logging.info(record)
        cur.execute('select count(*) from peak_accel')  # warnings or peak_accel
        for record in cur:
            logging.info(record)
        cur.execute('select distinct(device_id) from warnings')  # LIMIT 20
        cur.execute("select * from warnings where device_id = '005' order by warning_time")  # LIMIT 20
        # cur.execute("select count(device_id), acceleration_time from peak_accel where acceleration_time > '2020-01-05 04:48:00' group by acceleration_time order by acceleration_time LIMIT 500")  # peak_accel  or warnings
        # cur.execute("select * from peak_accel where acceleration_time = '2020-01-05 04:43:00' order by device_id LIMIT 400")  # peak_accel  or warnings

        rec_to_plot = pd.read_sql_query("select * from peak_accel where device_id = '005' order by acceleration_time limit 400", conn)  #cur.fetchall()

        rec_to_plot.plot(y='peak_acceleration', x='acceleration_time', title='device=005')
        cur.execute("select * from warnings where device_id = '001' order by warning_time")  # LIMIT 20
        rec_to_plot = pd.read_sql_query("select * from peak_accel where device_id = '007' order by acceleration_time limit 400", conn)  #cur.fetchall()
        a = rec_to_plot.plot(y='peak_acceleration', x='acceleration_time', title='device=007')
        rec_to_plot = pd.read_sql_query("select * from peak_accel where device_id = '016' and acceleration_time >= '2020-01-05 04:40:00' order by acceleration_time limit 500", conn)  #cur.fetchall()
        rec_to_plot.plot(y='peak_acceleration', x='acceleration_time', title='device=016')
        for record in cur:
            logging.info(record)
    except psycopg2.Error as e:
        logging.error(e)
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
