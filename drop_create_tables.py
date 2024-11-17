import sqlite3

# Define the database connection (replace with your database file path)
# SQLite database setup
db_path = "/Users/gokcerbelgusen/IdeaProjects/Sample Database/identifier.sqlite"


# SQL queries to drop and create tables
sql_queries = [
    "DROP TABLE IF EXISTS Fund_Assets;",
    "DROP TABLE IF EXISTS Fund_History;",
    "DROP TABLE IF EXISTS Fund_Details;",
    
    """
    CREATE TABLE Fund_Details (
        "Code" TEXT PRIMARY KEY,
        "Title" TEXT,
        "Category" TEXT,
        "Rank" TEXT,
        "Market_Share" TEXT,
        "ISIN_Code" TEXT,
        "Start_Time" TEXT,
        "End_Time" TEXT,
        "Value_Date" INTEGER,
        "Back_Value_Date" INTEGER,
        "Status" TEXT,
        "KAP_URL" TEXT
    );
    """,
    
    """
    CREATE TABLE Fund_History (
        "Fund_Code" TEXT,
        "Date" DATE,
        "Timestamp" INTEGER,
        "Price" REAL,
        "Market_Cap" REAL,
        "Number_Of_Shares" REAL,
        "Number_Of_Investors" INTEGER,
        FOREIGN KEY ("Fund_Code") REFERENCES Fund_Details("Code")
    );
    """,
    
    """
    CREATE TABLE Fund_Assets (
        "Fund_Code" TEXT,
        "Date" DATE,
        "Code" TEXT,
        "Percent" REAL,
        "Name" TEXT,
        FOREIGN KEY ("Fund_Code") REFERENCES Fund_Details("Code"),
        FOREIGN KEY ("Date") REFERENCES Fund_History("Date")
    );
    """
]

# Connect to the SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Execute each SQL query to drop and create the tables
for query in sql_queries:
    cursor.execute(query)

# Commit changes and close the connection
conn.commit()
conn.close()

print("Tables have been dropped and recreated successfully.")
