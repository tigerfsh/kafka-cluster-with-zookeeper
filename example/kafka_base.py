import json
import time

from confluent_kafka import Producer, KafkaError, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from logging import getLogger, basicConfig, Formatter, INFO
from logging.handlers import TimedRotatingFileHandler
from os.path import splitext, basename
from time import sleep

basicConfig(
    level=INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[TimedRotatingFileHandler(('logs/%s.log' % "my-kafka"), when='D')],
)
logger = getLogger("Kafka-test")

kafka_address = "localhost:9092"

class BaseKafkaProducer(object):
    """
    生产者基类
    """

    def __init__(self, topic):
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': kafka_address,
            'log.connection.close': False
        })

        self.logger = logger

        self.create_topic()
        self.partitions = self.producer.list_topics(
        ).topics[self.topic].partitions

    def create_topic(self, num_partitions=3, replication_factor=1):
        if self.topic not in self.producer.list_topics().topics:
            ac = AdminClient({'bootstrap.servers': kafka_address})
            futmap = ac.create_topics(
                [NewTopic(self.topic, num_partitions, replication_factor)])
            time.sleep(2)
            self.logger.debug(f'[kafka]create new topic [{self.topic}]')

    def send_log(self, err, msg):
        if err:
            self.logger.error(f'[kafka]message delivered failed: {err}')
        else:
            self.logger.debug(
                f'[kafka]message delivered to {msg.topic()} [{msg.partition()}]')

    def get_target_partition_id(self, key):
        return hash(key) % len(self.partitions)

    def send(self, data, key, flush=True):
        target_partition_id = self.get_target_partition_id(key)
        self.producer.produce(
            self.topic, json.dumps(data).encode('utf-8'),
            partition=target_partition_id,
            callback=self.send_log
        )
        if flush:
            self.flush()
            self.logger.debug(
                f'[kafka]message had delivered, topic is {self.topic}; id is: [{key}]')

    def flush(self):
        self.producer.flush()


class BaseKafkaConsumer(object):
    """
    消费者基类
    """

    def __init__(self, topic, group_id):
        self.topic = topic
        self.consumer = Consumer({
            'bootstrap.servers': kafka_address,
            'group.id': group_id,
            'default.topic.config': {'auto.offset.reset': 'smallest'},
        })

        assert self.topic in self.consumer.list_topics().topics, \
            'Kafka.Consumer.init: not found topic[{0}]'.format(self.topic)

        self.consumer.subscribe([self.topic])

        self.logger = logger

    def run(self, callbacks=None):
        if callbacks is None:
            callbacks = []

        try:
            while True:
                msg = self.consumer.poll(1)
                if msg is None:
                    continue

                if not msg.error():
                    value = msg.value()
                    try:
                        data = json.loads(value.decode('utf-8'))
                        print(data)
                    except Exception as ex:
                        self.logger.error(
                            '[kafka]json.loads message failed: {0}\nvalue: {1}'.format(
                                ex, value)
                        )
                    else:
                        self.logger.info(
                            f'[kafka]message received from {msg.topic()} [{msg.partition()}] value: {value}'
                        )
                        for callback in callbacks:
                            callback(data)

                # 参考https://stackoverflow.com/questions/47215245/error-cb-in-confluent-kafka-python-producers-and-consumers
                # 作者的回复 对于_TRANSPORT 可以先忽略
                elif msg.error().code() not in [KafkaError._PARTITION_EOF, KafkaError._TRANSPORT]:
                    self.logger.error(
                        '[kafka]message received failed: {0}'.format(msg.error()))
                    break

        except KeyboardInterrupt as e:
            self.logger.error('[kafka]KeyboardInterrupt.')

        finally:
            self.consumer.close()
