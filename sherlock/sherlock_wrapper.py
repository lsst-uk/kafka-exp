# Kafka wrapper for Sherlock
# Usage:
#   python3 sherlock_wrapper.py <source topic> <destination topic>

from confluent_kafka import Consumer, KafkaError
from confluent_kafka import Producer
import sys

settings = {
    #'bootstrap.servers': '192.41.108.22:9092',
    'bootstrap.servers': '192.168.0.25:29092',
    'group.id': 'copy-topic',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

if (len(sys.argv) < 3):
    print ("Usage:\n  python3 sherlock_wrapper.py <source topic> <destination topic>")
    sys.exit(0)

source = sys.argv[1]
dest = sys.argv[2]

#source = 'ztf_20200404_programid1'
#dest = 'ztf_test'

c = Consumer(settings)
p = Producer(settings)
c.subscribe([source])
n = 0
o = -1
try:
    while n < maxmsgs:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            print ("Got message with offset " + str(msg.offset()))
            #p.produce(dest, value=msg.value())
            o = msg.offset()
        else:
            print ("Error")
        n += 1
finally:
    c.close()
    p.flush()

print ("Copied {:d} messages up to offset {:d}".format(n,o))
