# Work dir
`cd /opt/kafka/bin`
# Create topic
`kafka-topics.sh --create --topic test1 --replication-factor 3 --partitions 2 --zookeeper zoo1:2181`
# Produce
`echo "hello, world" |kafka-console-producer.sh --broker-list kafka1:9092 --topic test1`
# Consume
`kafka-console-consumer.sh --bootstrap-server kafka1:9092 --topic test1 --from-beginning`
