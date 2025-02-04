from hdfs import InsecureClient

import pandas as pd

from sqlalchemy import create_engine

import pymysql  # Required for MySQL connection

# HDFS Configuration

HDFS_URL = "http://master-node:9870"  # Change if needed

HDFS_FILE_PATH = "/user/hadoop/Final_product_data.csv"  # Adjust if needed

# Connect to HDFS

client = InsecureClient(HDFS_URL, user="hdfs")

try:

    with client.read(HDFS_FILE_PATH, encoding='utf-8') as reader:

        df = pd.read_csv(reader)

    # Display first few rows to verify
    print("✅ Data Loaded from HDFS:")
    print(df.head())

except Exception as e:
    print(f"❌ Error reading from HDFS: {e}")
    exit(1)# Stop execution if file loading fails
# MySQL Configuration
MYSQL_HOST = "100.120.200.31"  # MySQL Server IP
MYSQL_USER = "root"
MYSQL_PASSWORD = "123123"
MYSQL_DATABASE = "ProductDB"
MYSQL_TABLE = "Products"  # Name of the table
try:
    # Create MySQL connection
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}")
    # Verify connection
    with engine.connect() as connection:
        print("✅ Successfully connected to MySQL.")
    # Write DataFrame to MySQL
    df.to_sql(MYSQL_TABLE, con=engine, if_exists='replace', index=False)
    print(f"✅ Data successfully ingested into {MYSQL_DATABASE}.{MYSQL_TABLE}")
except Exception as e:
    print(f"❌ Error connecting to MySQL or writing data: {e}")
    exit(1)  # Stop execution if MySQL connection fails