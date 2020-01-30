import psycopg2
from configparser import ConfigParser

ConfigParser.read('config.cfg')

dbhost = ConfigParser.get('postgres', 'dbhost')
dbname = ConfigParser.get('postgres', 'dbname')
dbuser = ConfigParser.get('postgres', 'dbuser')
dbpassword = ConfigParser.get('postgres', 'dbpassword')

def main():
    # setup Postgres db and table
    # setup_db()

    conn = psycopg2.connect(f"host={dbhost} dbname={dbname} user={dbuser} password={dbpassword}")
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