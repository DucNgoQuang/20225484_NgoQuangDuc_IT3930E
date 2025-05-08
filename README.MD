
# 2024.2 IT3930E Project 2 - Lambda architecture in Big Data management 

This repository contain the source code for Project 2 (IT3930E) at HUST


## Prerequisites

- [Apache Kafka](https://kafka.apache.org/quickstart)
- [Apache Hadoop](https://hadoop.apache.org/releases.html)
- [Apache Spark](https://spark.apache.org/downloads.html)
- [Clickhouse](https://clickhouse.com/docs/install)
- [Apache Superset](https://superset.apache.org/)

## System architecture

<img src='asset/Project 2.png' alt='System architecture' > 

## Install required libraries
```bash
pip install -r requirements.txt
```

## Kafka Set Up 
### Start Kafka (KRaft mode)
```bash
bin/kafka-server-start.sh config/kraft/server.properties 
```

### Create topic
```bash
bin/kafka-topics.sh --create --topic retail_transactions --bootstrap-server localhost:9092 
```

### Start  producer
```bash
python producer.py
```

## Create table in Click house 
```sql
CREATE OR REPLACE TABLE retail_transactions
(
    CustomerID UInt32,
    ProductID String,
    Quantity UInt32,
    Price UInt32,
    TransactionDate DateTime,
    PaymentMethod String,
    Street String,
    City Nullable(String),
    State Nullable(String),
    ZipCode Nullable(UInt32),
    ProductCategory String,
    DiscountAppliedPercent Float32,
    TotalAmount Float32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(TransactionDate)
ORDER BY (ProductID, TransactionDate);


CREATE OR REPLACE TABLE historical_retail_transactions
(
    CustomerID UInt32,
    ProductID String,
    Quantity UInt32,
    Price UInt32,
    TransactionDate DateTime,
    PaymentMethod String,
    Street String,
    City Nullable(String),
    State Nullable(String),
    ZipCode Nullable(UInt32),
    ProductCategory String,
    DiscountAppliedPercent Float32,
    TotalAmount Float32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(TransactionDate)
ORDER BY (ProductID, TransactionDate);
```

## Spark streaming job submit 
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,com.clickhouse:clickhouse-jdbc:0.4.6 \
  speed_layer/spark_streaming.py
```
