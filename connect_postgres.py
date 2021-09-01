import psycopg2
from config import config

""" 
We first create a database.ini file that'll contain our connection parameters.
We'll then create a config file & use the config() function to read in the .ini file
We can now then connect to the database
We're going to connect to an empty sales Postgresql database
and create 4 tables; dsr(sales people), suppliers, items & item sales.
We'll then add values to the database & proceed to create some queries.

"""

# add database connect parameters into a dictionary variable
params = config()

# create a database class
class DatabaseInstance:
    """
    \nThis class initializes a database instance by using connection parameters parsed in
    from the database.ini file by the configuration file config.py.
    We then create a cursor object that we'll use to pass PostgreSQL queries.\n
    """
    # initialize database instance, check if params are correct as well.
    def __init__(self, **params):
        self.conn = None
        try:
            self.conn = psycopg2.connect(**params)
            self.cur = self.conn.cursor()
            print('Connected to PostgreSQL database:',params['database'])
        except (Exception, psycopg2.DatabaseError) as error:
            print(f'Error connecting to the database, error is\n{error}')
    
    # method to get database version
    def database_version(self, query):
        self.cur.execute(query)
        db_version = self.cur.fetchone()
        return db_version
    
    # method to access execute function of the cursor object
    def execute_single_query(self, query, data):
        self.cur.execute(query, data)
        self.conn.commit()
        return self.cur

    # method to access executemany function of the cursor objec
    def execute_multiple_query(self, query, data_list):
        self.cur.executemany(query, data_list)
        self.conn.commit()
        return self.cur
    
    # add tables to the database
    def create_table(self, table_query):
        for query in table_query:
            self.cur.execute(query)
            self.conn.commit()
        print('\nTables created successfully...')
        return self.cur

    # closes database session
    def __del__(self):
        if self.conn is not None:
            self.cur.close()
            self.conn.close()
            print(params['database'],'PostgreSQL database closed')



if __name__ == '__main__':

    # assign database instance
    sales_db = DatabaseInstance(**params)
    
    # checks if a database instance was started, if there were no connection errors
    if sales_db.conn is not None:
        sales_db.database_version('select version();')

        # SQL query to create a table(s)
        table_query = (
        """
        DROP TABLE IF EXISTS dsr CASCADE;
        CREATE TABLE dsr (
            dsr_id SERIAL PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50)
            );
        """,
        """
        DROP TABLE IF EXISTS suppliers CASCADE;
        CREATE TABLE suppliers (
            supplier_id SERIAL PRIMARY KEY,
            supplier_name VARCHAR(255) NOT NULL
        );
        """,
        """
        DROP TABLE IF EXISTS items CASCADE;
        CREATE TABLE items (
            item_id SERIAL PRIMARY KEY,
            item_name VARCHAR(255) NOT NULL,
            supplier_id INTEGER NOT NULL,
            FOREIGN KEY (supplier_id)
                REFERENCES  suppliers(supplier_id)
                ON UPDATE CASCADE ON DELETE CASCADE
        );
        """,
        """
        DROP TABLE IF EXISTS item_sales CASCADE;
        CREATE TABLE item_sales (
            item_id INTEGER NOT NULL,
            amount INT NOT NULL,
            dsr_id INT NOT NULL,
            sale_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (item_id, dsr_id),
            FOREIGN KEY (item_id)
                REFERENCES items (item_id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY (dsr_id)
                REFERENCES dsr (dsr_id)
                ON UPDATE CASCADE ON DELETE CASCADE
        );
        """
        )
        # create tables
        sales_db.create_table(table_query)
                
        print(DatabaseInstance.__doc__)
    else:
        exit()
