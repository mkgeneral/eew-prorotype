import psycopg2

dbhost = 'postgresql.cmp8s1zeudhi.us-west-2.rds.amazonaws.com'
dbname = 'eewdb'
dbuser = 'postgres'
dbpassword = 'oIDED14PpCSHHjjdg6sh'


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