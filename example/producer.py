import time 
import string
import random

from .kafka_base import BaseKafkaProducer, BaseKafkaConsumer

class TestProducer(BaseKafkaProducer):
    pass 

def random_string(num=8):
    s = string.ascii_letters + string.digits
    return "".join(random.sample(s, num))

if __name__ == "__main__":
    p = TestProducer("test1")
    while True:
        data = {
            "id": random_string(),
            "name": "test-1"
        }
        p.send(data, "my-key")