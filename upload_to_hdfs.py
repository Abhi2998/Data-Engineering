from hdfs import InsecureClient

# Define HDFS connection
hdfs_host = "http://100.120.200.31:9870"  # Your Hadoop Web UI
hdfs_path = "/user/hadoop/Final_product_data.csv"
local_file = "C:\\Users\\Dell\\Desktop\\Data Engineering\Final_product_data.csv"
# Initialize HDFS Client
client = InsecureClient(hdfs_host, user='master')

# Upload file to HDFS
try:
    client.upload(hdfs_path, local_file, overwrite=True)
    print(f"✅ File successfully uploaded to HDFS at {hdfs_path}")
except Exception as e:
    print(f"❌ Error uploading file to HDFS: {e}")
