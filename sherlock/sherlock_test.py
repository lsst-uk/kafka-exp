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
import fastavro
from io import BytesIO
import json

settings = {
    #'bootstrap.servers': '192.41.108.22:9092',
    'bootstrap.servers': '192.168.0.25:29092',
    'group.id': 'copy-topic',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

source_topic = 'ztf_test'
input_topic = 'dev_sherlock_test_input'
output_topic = 'dev_sherlock_test_output'

def ingest(maxmsgs):
    print ("Doing ingest")
    c = Consumer(settings)
    p = Producer(settings)
    c.subscribe([source_topic])
    n = 0
    try:
        while n < maxmsgs:
            # poll for messages
            msg = c.poll(0.1)
            if msg is None:
                continue
            elif not msg.error():
                print ("Got message with offset " + str(msg.offset()))
                decoded_msg = fastavro.reader(BytesIO(msg.value()))
                print ("Decoded message")
                for alert in decoded_msg:
                    # remove images
                    alert.pop('cutoutDifference')
                    alert.pop('cutoutTemplate')
                    alert.pop('cutoutScience')
                    #print (str(alert))
                    p.produce(input_topic, value=json.dumps(alert))
                    print ("Produced message on topic " + input_topic)
            else:
                print ("Error")
            n += 1
    finally:
        c.close()
        p.flush()
    return

def output(maxmsgs):
    print ("Getting output")    
    c = Consumer(settings)
    c.subscribe([output_topic])
    n = 0
    try:
        while n < maxmsgs:
            # poll for messages
            msg = c.poll(0.1)
            if msg is None:
                continue
            elif not msg.error():
                print ("Got message with offset " + str(msg.offset()))
                #print (msg.value())
                alert = json.loads(msg.value())
                print ("Got alert ra:{} dec:{} sum:{}".format(alert['candidate']['ra'],alert['candidate']['dec'],alert.get('sum')))
            else:
                print ("Error:" + str(msg))
            n += 1
    finally:
        c.close()
    return


if __name__ == '__main__':
    try:
        n = int(sys.argv[1])
    except IndexError:
        n = 10
    ingest(n)
    output(n)
