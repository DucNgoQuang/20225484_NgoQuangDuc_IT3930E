
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

### Start a producer
```bash
bin/kafka-console-producer.sh --topic retail_transactions --bootstrap-server localhost:9092 
```

## Spark streaming job submit 
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,com.clickhouse:clickhouse-jdbc:0.4.6 \
  speed_layer/spark_streaming.py
```
