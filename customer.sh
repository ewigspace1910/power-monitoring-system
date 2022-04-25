TOPIC="test"
IP="localhost:9092"

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 ./customer/processor.py  --t $TOPIC --ip $IP 
