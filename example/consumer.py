from .kafka_base import BaseKafkaProducer, BaseKafkaConsumer

class TestConsumer(BaseKafkaConsumer):
    pass 

if __name__ == "__main__":
    #
    c = TestConsumer("test1", "qiyuan.group")
    c.run()