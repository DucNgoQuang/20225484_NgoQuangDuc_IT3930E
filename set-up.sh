bin/kafka-server-start.sh config/kraft/server.properties &
bin/kafka-topics.sh --create --topic retail_transactions --bootstrap-server localhost:9092 & 

bin/kafka-console-producer.sh --topic retail_transactions --bootstrap-server localhost:9092 &

bin/kafka-console-consumer.sh --topic retail_transactions --bootstrap-server localhost:9092 --from-beginning &

spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,com.clickhouse:clickhouse-jdbc:0.4.6    spark_streaming.py

sudo clickhouse start
clickhouse-client --password

source superset-venv/bin/activate
export FLASK_APP=superset

'TransactionDate'
retail_transactions