# Work dir
`cd /opt/kafka/bin`
# Create topic
`kafka-topics.sh --create --topic test1 --replication-factor 2 --partitions 3 --zookeeper zoo1:2181`
# Produce
`for i in {1..10}; do echo "Hi, $i" |kafka-console-producer.sh --broker-list kafka1:9092 --topic test1; done`
# Consume
`kafka-console-consumer.sh --bootstrap-server kafka1:9092 --topic test1 --group group1`
# Descrebe
`./kafka-topics.sh --describe --zookeeper zoo1:2181 --topic test1`
