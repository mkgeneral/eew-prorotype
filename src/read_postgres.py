import psycopg2
from configparser import ConfigParser

config = ConfigParser()
config.read_file(open('config.cfg'))

DBHOST = config.get('POSTGRES', 'dbhost')
DBNAME = config.get('POSTGRES', 'dbname')
DBUSER = config.get('POSTGRES', 'dbuser')
DBPASSWORD = config.get('POSTGRES', 'dbpassword')

def main():
    # setup Postgres db and table
    # setup_db()

    conn = psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASSWORD}")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    try:
        cur.execute('select count(*) from peak_accel')
        for record in cur:
            print(record)
        cur.execute('select * from peak_accel LIMIT 20')
        for record in cur:
            print(record)
    except psycopg2.Error as e:
        print(e)


if __name__ == "__main__":
    main()