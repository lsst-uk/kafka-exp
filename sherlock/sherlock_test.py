# Test for using Sherlock via Kafka
# Usage:
#   python3 sherlock_test [n]
#
# Ingest reads n (default 10) messages from the ztf_test topic, decodes them and 
# sends (reduced) alerts to the dev_sherlock_test_input topic. Output reads from
# the dev_sherlock_test_output topic and prints a summary of each message.

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

#source = 'ztf_20200404_programid1'
source_topic = 'ztf_test'
input_topic = 'dev_sherlock_test_input'
output_topic = 'dev_sherlock_test_output'

def ingest():
    print ("do ingest")
    return

def output():
    print ("do output")    

#c = Consumer(settings)
#p = Producer(settings)
#c.subscribe([source])
#n = 0
#o = -1
#try:
#    while n < maxmsgs:
#        msg = c.poll(0.1)
#        if msg is None:
#            continue
#        elif not msg.error():
#            print ("Got message with offset " + str(msg.offset()))
#            #p.produce(dest, value=msg.value())
#            o = msg.offset()
#        else:
#            print ("Error")
#        n += 1
#finally:
#    c.close()
#    p.flush()
#
#print ("Copied {:d} messages up to offset {:d}".format(n,o))

if __name__ == '__main__':
    print("do something")
