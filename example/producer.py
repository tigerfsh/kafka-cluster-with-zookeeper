import time 
import string
import random

from kafka_base import BaseKafkaProducer, BaseKafkaConsumer

class TestProducer(BaseKafkaProducer):
    pass 

def random_string(num=8):
    s = string.ascii_letters + string.digits
    return "".join(random.sample(s, num))

if __name__ == "__main__":
    p = TestProducer("test1")
    while True:
        try:
            for i in range(1000):
                data = {
                    "id": random_string(),
                    "name": f"test_name_{i}"
                }
                p.send(data, "my-key")
        except KeyboardInterrupt as e:
            print("\nCtrl+C, Exit!")
            break