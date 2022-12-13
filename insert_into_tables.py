import psycopg2
from connect_postgres import DatabaseInstance, params

# create a class to insert data into database tables
class InsertIntoTables(DatabaseInstance):
    """Insert data into tables"""
    def __init__(self, **params):
        super().__init__(**params)

        # method to add dsr to the dsr table using the execute_single_query method earlier created
    def add_dsr(self, first_name, last_name):
        query = """
        INSERT INTO dsr (first_name, last_name)
        VALUES (%s, %s) 
        RETURNING dsr_id, first_name, last_name;
        """
        self.execute_single_query(query, (first_name, last_name))
        dsr = self.cur.fetchone()
        print(f"Added a new dsr:{dsr}")


    # add supplier to supplier table
    def add_supplier(self, supplier_name):
        query = """
        INSERT INTO suppliers (supplier_name)
        VALUES (%s)
        RETURNING supplier_id, supplier_name;
        """
        self.execute_single_query(query, (supplier_name,))
        supplier = self.cur.fetchone()
        print(f'\nAdded a new supplier:{supplier} \n')
    
    # add item to items table
    def add_item(self, item_name, supplier_id):
        query = """
        INSERT INTO items (item_name, supplier_id)
        VALUES (%s, %s)
        RETURNING item_id, item_name, supplier_id
        """
        self.execute_single_query(query, (item_name, supplier_id))
        item = self.cur.fetchone()
        print(f"Added item: {item}")

    # add item sales to item sales table
    def add_item_sales(self, item_id, amount, dsr_id):
        query = """
        INSERT INTO item_sales (item_id, amount, dsr_id)
        VALUES (%s, %s, %s)
        RETURNING item_id, amount, dsr_id, sale_date
        """
        self.execute_single_query(query, (item_id, amount, dsr_id))
        sales = self.cur.fetchone()
        print(f"Added new sales: {sales}")

    def __del__(self):
        return super().__del__()


insert_data = InsertIntoTables(**params)

if insert_data.conn is not None:

    insert_data.add_dsr('Rodney', 'Trotter')

    insert_data.add_supplier('Hill')

    insert_data.add_item('Sugar', 1)

    insert_data.add_item_sales(1, 200, 1)
    
    insert_data.conn.close()
