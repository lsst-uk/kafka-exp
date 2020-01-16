# Convert avro to json stream
#
from confluent_kafka import Consumer, KafkaError
import fastavro
from io import BytesIO
import os
import logging

settings = {
    'bootstrap.servers': '192.41.108.22:9092',
    'group.id': 'testcongroup',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

logging.basicConfig(level=logging.ERROR)
logging.getLogger("requests").setLevel(logging.CRITICAL)
logging.getLogger("swiftclient").setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)

maxmsgs = 1
n = 0
topic = 'ztf_20200103_programid1'

c = Consumer(settings)
c.subscribe([topic])
try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            decoded_msg = fastavro.reader(BytesIO(msg.value()))
            for alert in decoded_msg:
                # convert to json
                print ('') 
            n += 1
            if n >= maxmsgs:
                break
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            print('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass
finally:
    c.close()

print ("Got %d messages" % (n))

