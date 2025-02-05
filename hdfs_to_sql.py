from hdfs import InsecureClient
import pandas as pd
from sqlalchemy import create_engine
import pymysql  # Required for MySQL connection


# ✅ HDFS Configuration
HDFS_URL = "http://master-node:9870"  # Adjust if needed
HDFS_FILES = {
    "/user/hadoop/Products.csv": "products_table",
    "/user/hadoop/Pricing.csv": "pricing_table",
    "/user/hadoop/Categories.csv": "categories_table",
    "/user/hadoop/Reviews.csv":"reviews_table",
    "/user/hadoop/Shipping.csv":"shipping.table"
}  # Mapping of HDFS file to custom MySQL table name


# ✅ MySQL Configuration
MYSQL_HOST = "100.120.200.31"  # MySQL Server IP
MYSQL_USER = "root"
MYSQL_PASSWORD = "123123"
MYSQL_DATABASE = "ProductDB"


# ✅ Connect to HDFS
client = InsecureClient(HDFS_URL, user="hdfs")


# ✅ Connect to MySQL
try:
    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}")


    with engine.connect() as connection:
        print("✅ Successfully connected to MySQL.")


    # ✅ Process each file in HDFS
    for file_path, table_name in HDFS_FILES.items():
        try:
            # Read CSV file from HDFS
            with client.read(file_path, encoding='utf-8') as reader:
                df = pd.read_csv(reader)


            # ✅ Custom logic to modify table name (optional)
            # Example: Adding "data_" prefix or changing naming format
            table_name = f"data_{table_name}"  # Example of adding prefix


            # ✅ Write DataFrame to MySQL
            df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            print(f"✅ Data from {file_path} successfully ingested into {MYSQL_DATABASE}.{table_name}")


        except Exception as e:
            print(f"❌ Error processing {file_path}: {e}")


except Exception as e:
    print(f"❌ Error connecting to MySQL: {e}")
