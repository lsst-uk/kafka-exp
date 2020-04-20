# Kafka wrapper for Sherlock
# Usage:
#   python3 sherlock_wrapper.py <source topic> <destination topic>

from confluent_kafka import Consumer, KafkaError
from confluent_kafka import Producer
import sys
from mock_sherlock_driver import run_sherlock
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

maxmsgs = 1

def wrapper(input_topic, output_topic):
    print("Input topic is {}, output topic is {}".format(input_topic,output_topic))
    c = Consumer(settings)
    p = Producer(settings)
    c.subscribe([input_topic])
    n = 0
    try:
        while n < maxmsgs:
            msg = c.poll(0.1)
            if msg is None:
                continue
            elif not msg.error():
                print ("Got message with offset " + str(msg.offset()))
                alert = json.loads(msg.value())
                # sherlock expects a name attribute
                # if we don't have one then make it up
                alert['name'] = alert.get('name', alert.get('objectId', alert.get('candid', 'XXX12345')))
                #print (msg.value())
                query = []
                query.append({
                    'name': alert['name'],
                    'ra': alert['candidate']['ra'],
                    'dec': alert['candidate']['dec']
                    })
                response_str = run_sherlock(json.dumps(query))
                print ("Sherlock response: " + response_str)
                response = json.loads(response_str)
                alert['sum'] = response[alert['name']]['sum']
                p.produce(output_topic, value=json.dumps(alert))
                print ("Produced output on " + output_topic)
            else:
                print ("Error")
            n += 1
    finally:
        c.close()
        p.flush()
    return

if __name__ == '__main__':
    try:
        input_topic = sys.argv[1]
    except IndexError:
        input_topic = 'dev_sherlock_test_input'
    try:
        output_topic = sys.argv[2]
    except IndexError: 
        output_topic = 'dev_sherlock_test_output'
    wrapper(input_topic, output_topic)

