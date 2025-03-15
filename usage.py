# Initialize database handler
db_handler = AccessDBHandler("C:\\Users\\Vikram\\Documents\\my_database.accdb")

# Load data from Excel file
df = pd.read_excel("C:\\Users\\Vikram\\Documents\\data.xlsx", engine='openpyxl')

# Create a table dynamically from the DataFrame schema
db_handler.create_table("MyTable", df)

# Add a column to the table
db_handler.add_column("MyTable", "NewColumn", "TEXT")

# Change the datatype of a column
db_handler.change_column_type("MyTable", "ExistingColumn", "DOUBLE")

# Delete a column from the table
db_handler.delete_column("MyTable", "OldColumn")

# Insert data from Excel while checking for duplicates
db_handler.insert_data_from_excel("MyTable", "C:\\Users\\Vikram\\Documents\\data.xlsx", ["date", "market", "month", "year", "week"])

# Close the database connection
db_handler.close_connection()
