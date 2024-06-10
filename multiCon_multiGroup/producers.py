from confluent_kafka import Producer
import random
import random
import string



me="Marwa-2"
conf = {'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094',
        'client.id': me}
for i in range(100):
        msg="Hello" + str(i)
        key = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(4))
        producer = Producer(conf)
        producer.produce(me, key=key, value=msg)
        producer.flush()