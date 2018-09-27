import json, time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from random import uniform

consumer = KafkaConsumer(bootstrap_servers=['172.24.41.165:8081'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
consumer.subscribe(pattern='.*.*.*.-JuanPerez')

producer = KafkaProducer(bootstrap_servers=['172.24.41.165:8081'], 
             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for message in consumer:
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))





