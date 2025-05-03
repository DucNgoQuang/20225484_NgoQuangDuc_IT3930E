import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import from_json, split, trim, to_timestamp, col
from dotenv import load_dotenv

spark = SparkSession.builder \
    .appName("DataFrame Example") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()



schema = StructType([
    StructField('CustomerID', IntegerType(), True),
    StructField("ProductID", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Price", FloatType(), True),
    StructField("TransactionDate", StringType(), True),
    StructField("PaymentMethod", StringType(), True),
    StructField("StoreLocation", StringType(), True),
    StructField("ProductCategory", StringType(), True),
    StructField("DiscountApplied(%)", FloatType(), True),
    StructField("TotalAmount", FloatType(), True),
])

spark.sparkContext.setLogLevel("WARN")  

date = datetime.datetime.now().strftime('%d%m%y')
file_path = f"hdfs://127.0.0.1:9871/user/hdoop/retail_transactions_{date}/*.csv"

df = spark.read.csv(
    file_path,
    schema=schema,
    multiLine=True,
    quote="\"",
    escape="\"",
    header= False
)


df = df.withColumn("TransactionDate", to_timestamp(col("TransactionDate"), format="M/d/yyyy HH:mm:ss")) \
                               .withColumn("Street", trim(split("StoreLocation", "\n").getItem(0))) \
                               .withColumn("CityStateZip", trim(split("StoreLocation", "\n").getItem(1)))

df = df.withColumn("City", trim(split("CityStateZip", ",").getItem(0))) \
                               .withColumn("StateZip", trim(split("CityStateZip", ",").getItem(1)))

df = df.withColumn("State", trim(split("StateZip", " ").getItem(0))) \
                                .withColumn("ZipCode", trim(split("StateZip", " ").getItem(1)).cast(IntegerType()))

df = df.drop("StoreLocation") \
                                .drop("CityStateZip") \
                                .drop("StateZip") \
                                .withColumnRenamed("DiscountApplied(%)", "DiscountAppliedPercent" ) \



spark.stop()