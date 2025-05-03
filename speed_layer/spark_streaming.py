from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DayTimeIntervalType
from pyspark.sql.functions import from_json, split, trim, to_timestamp, col
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path='.env')

KAFKA_HOST =  os.getenv("KAFKA_HOST")
KAFKA_BROKER_PORT = os.getenv("KAFKA_BROKER_PORT")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

# schema for the data on broker
json_schema = StructType([
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

spark = SparkSession.builder \
    .appName("RetailTransactions") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
# Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "retail_transactions") \
    .load() 

json_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as msg_value")

json_schema_df = json_df.withColumn("msg_value", from_json(json_df["msg_value"], json_schema)).select("msg_value.*")

json_schema_df = json_schema_df.withColumn("TransactionDate", to_timestamp(col("TransactionDate"), format="M/d/yyyy HH:mm:ss")) \
                               .withColumn("Street", trim(split("StoreLocation", "\n").getItem(0))) \
                               .withColumn("CityStateZip", trim(split("StoreLocation", "\n").getItem(1)))

json_schema_df = json_schema_df.withColumn("City", trim(split("CityStateZip", ",").getItem(0))) \
                               .withColumn("StateZip", trim(split("CityStateZip", ",").getItem(1)))

json_schema_df = json_schema_df.withColumn("State", trim(split("StateZip", " ").getItem(0))) \
                                .withColumn("ZipCode", trim(split("StateZip", " ").getItem(1)).cast(IntegerType()))

json_schema_df = json_schema_df.drop("StoreLocation") \
                                .drop("CityStateZip") \
                                .drop("StateZip") \
                                .withColumnRenamed("DiscountApplied(%)", "DiscountAppliedPercent" ) \

def ForEachBatch_func(df, epoch_id): 
    df.write.format("jdbc") \
        .mode("append") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("url", f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}") \
        .option("dbtable", "default.retail_transactions") \
        .option("user", f"{CLICKHOUSE_USER}") \
        .option("password", f"{CLICKHOUSE_PASSWORD}") \
        .save()

# total_revenue_by_productid = json_schema_df.groupBy(["ProductID", "City" ,"State"]).agg({"TotalAmount": "sum"}).withColumnRenamed("sum(TotalAmount)", "TotalRevenueByProductID").sort("TotalRevenueByProductID", ascending=False)
# total_quantity_by_productid = json_schema_df.groupBy("ProductID").agg({"Quantity": "sum"}).withColumnRenamed("sum(Quantity)", "TotalQuantityByProductID").sort("TotalQuantityByProductID", ascending=False)


# query = total_revenue_by_productid.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", "false") \
#     .option("numRows", 50) \
#     .start()

# query.awaitTermination()

writing_df = json_schema_df.writeStream \
    .foreachBatch(ForEachBatch_func) \
    .start()


writing_df.awaitTermination()
