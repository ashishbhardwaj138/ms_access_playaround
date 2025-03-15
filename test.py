import pyodbc
import pandas as pd
import os

class AccessDBHandler:
    def __init__(self, db_path):
        """Initialize connection to the Access database."""
        self.db_path = db_path
        self.conn = self.connect_db()
    
    def connect_db(self):
        """Connect to the Access database or create one if it doesn’t exist."""
        if not os.path.exists(self.db_path):
            print(f"Database {self.db_path} does not exist. Creating a new one...")
        
        conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={self.db_path};'
        try:
            conn = pyodbc.connect(conn_str)
            print(f"Connected to database: {self.db_path}")
            return conn
        except Exception as e:
            print("Error connecting to database:", e)
            return None

    def create_table(self, table_name, df):
        """Create a table dynamically based on the DataFrame schema."""
        cursor = self.conn.cursor()

        columns = []
        for col, dtype in zip(df.columns, df.dtypes):
            col_type = self.get_sql_type(dtype)
            columns.append(f"[{col}] {col_type}")

        columns_str = ", ".join(columns)
        sql_query = f"CREATE TABLE {table_name} ({columns_str});"
        
        try:
            cursor.execute(sql_query)
            self.conn.commit()
            print(f"Table {table_name} created successfully.")
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")

    def get_sql_type(self, dtype):
        """Convert Pandas dtype to MS Access SQL type."""
        if "int" in str(dtype):
            return "INTEGER"
        elif "float" in str(dtype):
            return "DOUBLE"
        elif "datetime" in str(dtype):
            return "DATETIME"
        else:
            return "TEXT"

    def add_column(self, table_name, column_name, column_type, position=None):
        """Add a new column to the table at a specific position (if supported)."""
        cursor = self.conn.cursor()
        try:
            alter_query = f"ALTER TABLE {table_name} ADD COLUMN [{column_name}] {column_type};"
            cursor.execute(alter_query)
            self.conn.commit()
            print(f"Added column {column_name} to {table_name}.")
        except Exception as e:
            print(f"Error adding column {column_name}: {e}")

    def change_column_type(self, table_name, column_name, new_type):
        """Change the datatype of an existing column."""
        cursor = self.conn.cursor()
        try:
            temp_col = f"{column_name}_temp"
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN [{temp_col}] {new_type};")
            cursor.execute(f"UPDATE {table_name} SET [{temp_col}] = [{column_name}];")
            cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN [{column_name}];")
            cursor.execute(f"ALTER TABLE {table_name} RENAME COLUMN [{temp_col}] TO [{column_name}];")
            self.conn.commit()
            print(f"Changed data type of {column_name} to {new_type}.")
        except Exception as e:
            print(f"Error changing column {column_name}: {e}")

    def delete_column(self, table_name, column_name):
        """Delete a column from the table."""
        cursor = self.conn.cursor()
        try:
            alter_query = f"ALTER TABLE {table_name} DROP COLUMN [{column_name}];"
            cursor.execute(alter_query)
            self.conn.commit()
            print(f"Deleted column {column_name} from {table_name}.")
        except Exception as e:
            print(f"Error deleting column {column_name}: {e}")

    def check_duplicates(self, table_name, df, unique_columns):
        """Check if records already exist in the table."""
        cursor = self.conn.cursor()

        for index, row in df.iterrows():
            conditions = " AND ".join([f"[{col}] = '{row[col]}'" for col in unique_columns])
            query = f"SELECT COUNT(*) FROM {table_name} WHERE {conditions};"
            cursor.execute(query)
            count = cursor.fetchone()[0]

            if count > 0:
                print(f"Duplicate found: {row.to_dict()}")
                return True  # At least one duplicate found

        return False

    def insert_data_from_excel(self, table_name, file_path, unique_columns):
        """Insert data from an Excel file into the database, checking for duplicates first."""
        df = pd.read_excel(file_path, engine='openpyxl')

        if self.check_duplicates(table_name, df, unique_columns):
            print("Duplicate records detected. Skipping insertion.")
            return

        cursor = self.conn.cursor()
        placeholders = ", ".join(["?" for _ in df.columns])
        column_names = ", ".join([f"[{col}]" for col in df.columns])

        insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders});"

        for _, row in df.iterrows():
            cursor.execute(insert_query, tuple(row))

        self.conn.commit()
        print(f"Data from {file_path} inserted successfully into {table_name}.")

    def close_connection(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            print("Database connection closed.")
