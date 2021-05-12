1. Create topic: kafka-topics.sh --create --topic test1 --replication-factor 3 --partitions 2 --zookeeper
2. Produce: echo "hello, world" |kafka-console-producer.sh --broker-list kafka1:9092 --topic test1
3. Consume: kafka-console-consumer.sh --bootstrap-server kafka1:9092 --topic test1 --from-beginning
