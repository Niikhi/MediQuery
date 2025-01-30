import sqlite3

# Path to your SQLite database file
db_path = "medical_database.db"

# Connect to the database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Fetch and list all tables in the database
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

print("Tables:", tables)

# Display data from each table
for table in tables:
    table_name = table[0]
    print(f"\nData from table: {table_name}")
    try:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 10;")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except sqlite3.OperationalError as e:
        print(f"Could not fetch data from table {table_name}: {e}")

# Close the connection
conn.close()
