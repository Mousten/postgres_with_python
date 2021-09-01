import psycopg2
from config import config


def connect():
    conn = None
    try:
        # reading from connection parameters
        params = config()

        # connect to postgres 
        print('Connecting to PostgreSQL database...')
        print(f'This is {params}')
        conn = psycopg2.connect(**params)

        # create a cursor from connection object
        cur = conn.cursor()

        # execute a script
        print('PostgreSQL database version:')
        cur.execute('select version();')

        db_version = cur.fetchone()
        print(db_version)

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

if __name__ == '__main__':
    connect()
