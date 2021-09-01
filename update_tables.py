import psycopg2
from connect_postgres import DatabaseInstance, params

class UpdateDatabaseTables(DatabaseInstance):
    """ Update tables in the selected database"""
    def __init__(self, **params):
        super().__init__(**params)

    # show database version
    def print_version(self):
        self.cur.execute('select version();')
        version = self.cur.fetchall()
        print(version)

    # list tables in the selected database
    def show_tables(self):
        query = """
        SELECT *
        FROM pg_catalog.pg_tables
        WHERE schemaname != 'pg_catalog' AND
            schemaname != 'information_schema';
        """
        self.cur.execute(query)
        tables = self.cur.fetchall()

        print('\nShowing tables in', params['database'], 'database')

        for table in tables:
            print(table[1])

    # update a supplier's name
    def update_supplier_name(self, supplier_name, supplier_id):
        query = """
        UPDATE suppliers
        SET supplier_name = %s
        WHERE supplier_id = %s
        """
        self.execute_single_query(query, (supplier_name, supplier_id))
        updated_rows = self.cur.rowcount
        print(f"Updated row {updated_rows}")

    def __del__(self):
        return super().__del__()

if __name__ == '__main__':
    update_table = UpdateDatabaseTables(**params)

    if update_table.conn is not None:
        update_table.print_version()

        update_table.show_tables()

        update_table.update_supplier_name('Hill Corporation', 1)

        print(UpdateDatabaseTables.__doc__)

