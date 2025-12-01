from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, FloatType, LongType, StructType,StructField, StringType

# Create new Catalog with Spark + S3/MinIO
# To insert in local filesystem -> .config("spark.sql.catalog.iceberg_catalog.warehouse", "/home/iceberg/iceberg_warehouse")\

spark = SparkSession.builder \
    .appName("iceberg_catalog") \
    .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
    .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

schema = StructType([
  StructField("vendor_id", LongType(), True),
  StructField("trip_id", LongType(), True),
  StructField("trip_distance", FloatType(), True),
  StructField("fare_amount", DoubleType(), True),
  StructField("store_and_fwd_flag", StringType(), True)
])

if not spark.catalog.tableExists("iceberg_catalog.nyc.taxis"):
    df_empty = spark.createDataFrame([], schema)
    df_empty.writeTo("iceberg_catalog.nyc.taxis").create()

# Check Catalog has been successfully created
spark.sql("SHOW CATALOGS").show()

# Insert Data
schema = spark.table("iceberg_catalog.nyc.taxis").schema
data = [
    (1, 1000371, 1.8, 15.32, "N"),
    (2, 1000372, 2.5, 22.15, "N"),
    (2, 1000373, 0.9, 9.01, "N"),
    (1, 1000374, 8.4, 42.13, "Y")
  ]
df = spark.createDataFrame(data, schema)
df.writeTo("iceberg_catalog.nyc.taxis").append()