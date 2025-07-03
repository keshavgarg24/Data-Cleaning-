import psycopg2

# ✅ Update these credentials
DB_HOST = "localhost"
DB_PORT = "5433"
DB_NAME = "demodb"  # Replace with your actual database name
DB_USER = "postgres"
DB_PASSWORD = "0707"

try:
    # ✅ Connect to PostgreSQL database
    connection = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    cursor = connection.cursor()
    print("✅ PostgreSQL Connection Successful!")

    # ✅ Execute a test query
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    tables = cursor.fetchall()
    print("📋 Tables in the database:")
    for table in tables:
        print(f" - {table[0]}")

    # ✅ Close connection
    cursor.close()
    connection.close()
    print("✅ Connection Closed.")

except Exception as e:
    print(f"❌ Error connecting to PostgreSQL: {e}")
