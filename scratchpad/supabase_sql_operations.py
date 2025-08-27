import pg8000
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Fetch variables
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

# Connect to the database
try:
    connection = pg8000.Connection(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        database=DBNAME
    )
    print("Connection successful!")
    
    # Create a cursor to execute SQL queries
    cursor = connection.cursor()

    # delete table if exists
    delete_table_query = "DROP TABLE IF EXISTS temp;"
    cursor.execute(delete_table_query)
    connection.commit()
    print("Table 'temp' dropped successfully!")


    # delete the schema learning if exists (with CASCADE to drop all dependent objects)
    delete_schema_query = "DROP SCHEMA IF EXISTS learning_schema CASCADE;"
    cursor.execute(delete_schema_query)
    connection.commit()
    print("Schema 'learning_schema' and all its objects dropped successfully!")
    
    # 1. Create temp table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS temp (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        email VARCHAR(255),
        age INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_query)
    connection.commit()
    print("Table 'temp' created successfully!")
    
    # 2. Insert data (CREATE operation)
    print("\n--- INSERT Operations ---")
    insert_query = """
    INSERT INTO temp (name, email, age) VALUES 
    ('John Doe', 'john@example.com', 25),
    ('Jane Smith', 'jane@example.com', 30),
    ('Bob Johnson', 'bob@example.com', 35),
    ('Alice Brown', 'alice@example.com', 28);
    """
    cursor.execute(insert_query)
    connection.commit()
    print("Data inserted successfully!")
    
    # 3. Select data (READ operation)
    print("\n--- SELECT Operations ---")
    
    # Select all records
    cursor.execute("SELECT * FROM temp;")
    all_records = cursor.fetchall()
    print("All records:")
    for record in all_records:
        print(f"ID: {record[0]}, Name: {record[1]}, Email: {record[2]}, Age: {record[3]}, Created: {record[4]}")
    
    # Select specific columns
    cursor.execute("SELECT name, email FROM temp WHERE age > 25;")
    filtered_records = cursor.fetchall()
    print("\nNames and emails of people older than 25:")
    for record in filtered_records:
        print(f"Name: {record[0]}, Email: {record[1]}")
    
    # Count records
    cursor.execute("SELECT COUNT(*) FROM temp;")
    count = cursor.fetchone()[0]
    print(f"\nTotal records: {count}")
    
    # 4. Update data (UPDATE operation)
    print("\n--- UPDATE Operations ---")
    update_query = "UPDATE temp SET age = age + 1 WHERE name = 'John Doe';"
    cursor.execute(update_query)
    connection.commit()
    print("Updated John Doe's age (increased by 1)")
    
    # Show the updated record
    cursor.execute("SELECT * FROM temp WHERE name = 'John Doe';")
    updated_record = cursor.fetchone()
    print(f"Updated record: {updated_record}")
    
    # 5. Delete data (DELETE operation)
    print("\n--- DELETE Operations ---")
    delete_query = "DELETE FROM temp WHERE name = 'Bob Johnson';"
    cursor.execute(delete_query)
    connection.commit()
    print("Deleted Bob Johnson's record")
    
    # Show remaining records
    cursor.execute("SELECT * FROM temp;")
    remaining_records = cursor.fetchall()
    print("Remaining records after deletion:")
    for record in remaining_records:
        print(f"ID: {record[0]}, Name: {record[1]}, Email: {record[2]}, Age: {record[3]}")
    
    # 6. Alter table operations
    print("\n--- ALTER TABLE Operations ---")
    
    # Add a new column
    alter_add_column = "ALTER TABLE temp ADD COLUMN phone VARCHAR(20);"
    cursor.execute(alter_add_column)
    connection.commit()
    print("Added 'phone' column to the table")
    
    # Update the new column with some data
    update_phone = "UPDATE temp SET phone = '555-1234' WHERE name = 'John Doe';"
    cursor.execute(update_phone)
    connection.commit()
    print("Updated John Doe's phone number")
    
    # Modify column type
    alter_modify_column = "ALTER TABLE temp ALTER COLUMN age TYPE SMALLINT;"
    cursor.execute(alter_modify_column)
    connection.commit()
    print("Changed age column type to SMALLINT")
    
    # 7. Advanced SELECT operations
    print("\n--- Advanced SELECT Operations ---")
    
    # Order by
    cursor.execute("SELECT name, age FROM temp ORDER BY age DESC;")
    ordered_records = cursor.fetchall()
    print("Records ordered by age (descending):")
    for record in ordered_records:
        print(f"Name: {record[0]}, Age: {record[1]}")
    
    # Group by and aggregate functions
    cursor.execute("SELECT age, COUNT(*) as count FROM temp GROUP BY age;")
    grouped_records = cursor.fetchall()
    print("\nAge distribution:")
    for record in grouped_records:
        print(f"Age: {record[0]}, Count: {record[1]}")
    
    # 8. Index operations
    print("\n--- INDEX Operations ---")
    
    # Create different types of indexes
    print("Creating various types of indexes...")
    
    # 1. Single column index (already exists)
    create_index = "CREATE INDEX IF NOT EXISTS idx_temp_name ON temp(name);"
    cursor.execute(create_index)
    print("✓ Single column index on 'name'")
    
    # 2. Composite index (multiple columns)
    create_composite_index = "CREATE INDEX IF NOT EXISTS idx_temp_name_age ON temp(name, age);"
    cursor.execute(create_composite_index)
    print("✓ Composite index on 'name' and 'age'")
    
    # 3. Unique index
    create_unique_index = "CREATE UNIQUE INDEX IF NOT EXISTS idx_temp_email ON temp(email);"
    cursor.execute(create_unique_index)
    print("✓ Unique index on 'email'")
    
    # 4. Partial index (only for specific conditions)
    create_partial_index = "CREATE INDEX IF NOT EXISTS idx_temp_older_people ON temp(name) WHERE age > 30;"
    cursor.execute(create_partial_index)
    print("✓ Partial index on 'name' for people older than 30")
    
    connection.commit()
    
    # Show all indexes
    cursor.execute("""
        SELECT indexname, tablename, indexdef
        FROM pg_indexes 
        WHERE tablename = 'temp'
        ORDER BY indexname;
    """)
    indexes = cursor.fetchall()
    print("\nAll indexes on temp table:")
    for index in indexes:
        print(f"Index: {index[0]}")
        print(f"Definition: {index[2]}")
        print()
    
    # Demonstrate index benefits with timing
    print("--- INDEX BENEFITS DEMONSTRATION ---")
    
    # Insert more data to make the difference more noticeable
    insert_more_data = """
    INSERT INTO temp (name, email, age) VALUES 
    ('User1', 'user1@test.com', 22),
    ('User2', 'user2@test.com', 33),
    ('User3', 'user3@test.com', 44),
    ('User4', 'user4@test.com', 55),
    ('User5', 'user5@test.com', 66);
    """
    cursor.execute(insert_more_data)
    connection.commit()
    print("Added more test data for performance comparison")
    
    # Test query performance
    import time
    
    # Query without index optimization
    start_time = time.time()
    cursor.execute("SELECT * FROM temp WHERE name LIKE '%John%';")
    result1 = cursor.fetchall()
    time_without_opt = time.time() - start_time
    
    # Query with index optimization
    start_time = time.time()
    cursor.execute("SELECT name, email FROM temp WHERE name = 'John Doe';")
    result2 = cursor.fetchall()
    time_with_opt = time.time() - start_time
    
    print(f"Query time without optimization: {time_without_opt:.4f} seconds")
    print(f"Query time with index optimization: {time_with_opt:.4f} seconds")
    print(f"Performance improvement: {time_without_opt/time_with_opt:.1f}x faster with index")
    
    # 9. Table information
    print("\n--- TABLE INFORMATION ---")
    
    # Show table structure
    cursor.execute("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = 'temp'
        ORDER BY ordinal_position;
    """)
    columns = cursor.fetchall()
    print("Table structure:")
    for column in columns:
        print(f"Column: {column[0]}, Type: {column[1]}, Nullable: {column[2]}, Default: {column[3]}")
    
    # 10. Schema Operations
    print("\n--- SCHEMA OPERATIONS ---")
    
    # Create a new schema
    create_schema = "CREATE SCHEMA IF NOT EXISTS learning_schema;"
    cursor.execute(create_schema)
    connection.commit()
    print("✓ Created schema 'learning_schema'")
    
    # Create table in the new schema
    create_table_in_schema = """
    CREATE TABLE IF NOT EXISTS learning_schema.users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(50) UNIQUE NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_in_schema)
    connection.commit()
    print("✓ Created 'users' table in 'learning_schema'")
    
    # Create another table in the schema
    create_orders_table = """
    CREATE TABLE IF NOT EXISTS learning_schema.orders (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES learning_schema.users(id),
        product_name VARCHAR(100) NOT NULL,
        amount DECIMAL(10,2) NOT NULL,
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_orders_table)
    connection.commit()
    print("✓ Created 'orders' table in 'learning_schema'")
    
    # Insert data into schema tables
    insert_users = """
    INSERT INTO learning_schema.users (username, email) VALUES 
    ('john_doe', 'john@example.com'),
    ('jane_smith', 'jane@example.com'),
    ('bob_wilson', 'bob@example.com');
    """
    cursor.execute(insert_users)
    connection.commit()
    print("✓ Inserted users into schema table")
    
    # Insert orders with foreign key references
    insert_orders = """
    INSERT INTO learning_schema.orders (user_id, product_name, amount) VALUES 
    (1, 'Laptop', 999.99),
    (1, 'Mouse', 25.50),
    (2, 'Keyboard', 75.00),
    (3, 'Monitor', 299.99);
    """
    cursor.execute(insert_orders)
    connection.commit()
    print("✓ Inserted orders into schema table")
    
    # Query data from schema tables
    print("\n--- QUERYING SCHEMA TABLES ---")
    
    # Join tables across schema
    join_query = """
    SELECT u.username, o.product_name, o.amount, o.order_date
    FROM learning_schema.users u
    JOIN learning_schema.orders o ON u.id = o.user_id
    ORDER BY o.order_date DESC;
    """
    cursor.execute(join_query)
    join_results = cursor.fetchall()
    print("User orders:")
    for record in join_results:
        print(f"  {record[0]} bought {record[1]} for ${record[2]} on {record[3]}")
    
    # Show all schemas in the database
    print("\n--- SCHEMA INFORMATION ---")
    cursor.execute("""
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schema_name;
    """)
    schemas = cursor.fetchall()
    print("Available schemas:")
    for schema in schemas:
        print(f"  - {schema[0]}")
    
    # Show tables in our schema
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'learning_schema'
        ORDER BY table_name;
    """)
    schema_tables = cursor.fetchall()
    print("\nTables in 'learning_schema':")
    for table in schema_tables:
        print(f"  - {table[0]}")
    
    # Create a view in the schema
    create_view = """
    CREATE OR REPLACE VIEW learning_schema.user_order_summary AS
    SELECT 
        u.username,
        COUNT(o.id) as total_orders,
        SUM(o.amount) as total_spent,
        AVG(o.amount) as avg_order_value
    FROM learning_schema.users u
    LEFT JOIN learning_schema.orders o ON u.id = o.user_id
    GROUP BY u.id, u.username
    ORDER BY total_spent DESC;
    """
    cursor.execute(create_view)
    connection.commit()
    print("\n✓ Created view 'user_order_summary' in schema")
    
    # Query the view
    cursor.execute("SELECT * FROM learning_schema.user_order_summary;")
    view_results = cursor.fetchall()
    print("\nUser order summary:")
    for record in view_results:
        print(f"  {record[0]}: {record[1]} orders, ${record[2]:.2f} total, ${record[3]:.2f} avg")
    
    # Create a function in the schema
    create_function = """
    CREATE OR REPLACE FUNCTION learning_schema.get_user_orders(p_username VARCHAR)
    RETURNS TABLE(product_name VARCHAR, amount DECIMAL, order_date TIMESTAMP) AS $$
    BEGIN
        RETURN QUERY
        SELECT o.product_name, o.amount, o.order_date
        FROM learning_schema.users u
        JOIN learning_schema.orders o ON u.id = o.user_id
        WHERE u.username = p_username
        ORDER BY o.order_date DESC;
    END;
    $$ LANGUAGE plpgsql;
    """
    cursor.execute(create_function)
    connection.commit()
    print("\n✓ Created function 'get_user_orders' in schema")
    
    # Test the function
    cursor.execute("SELECT * FROM learning_schema.get_user_orders('john_doe');")
    function_results = cursor.fetchall()
    print("\nJohn Doe's orders (via function):")
    for record in function_results:
        print(f"  {record[0]}: ${record[1]} on {record[2]}")
    
    # Set search path to use schema by default
    set_search_path = "SET search_path TO learning_schema, public;"
    cursor.execute(set_search_path)
    print("\n✓ Set search path to include 'learning_schema'")
    
    # Now we can query without schema prefix
    cursor.execute("SELECT username, email FROM users LIMIT 3;")
    search_path_results = cursor.fetchall()
    print("\nUsers (using search path):")
    for record in search_path_results:
        print(f"  {record[0]}: {record[1]}")
    
    # Example query
    cursor.execute("SELECT NOW();")
    result = cursor.fetchone()
    print("Current Time:", result)


    # set public as before as default
    set_search_path = "SET search_path TO public;"
    cursor.execute(set_search_path)
    print("\n✓ Set search path to include 'public'")

    # Close the cursor and connection
    cursor.close()
    connection.close()
    print("Connection closed.")

except Exception as e:
    print(f"Failed to connect: {e}")