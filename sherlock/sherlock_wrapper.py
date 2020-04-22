# Kafka wrapper for Sherlock
#
# Reads n alerts from an input topic, extracts ra and dec, gets sherlock
# output, adds that output back to the alert and writes to output topic 
#
# Usage:
#   python3 sherlock_wrapper.py [n=10]

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

def wrapper(input_topic, output_topic, maxmsgs):
    print("Input topic is {}, output topic is {}".format(input_topic,output_topic))
    c = Consumer(settings)
    p = Producer(settings)
    c.subscribe([input_topic])
    n = 0
    try:
        while n < maxmsgs:
            # Poll for messages
            msg = c.poll(0.1)
            if msg is None:
                continue
            elif not msg.error():
                print ("Got message with offset " + str(msg.offset()))
                alert = json.loads(msg.value())
                
                # sherlock expects a name attribute
                # if we don't have one then make it up
                name = alert.get('name', alert.get('objectId', alert.get('candid', 'XXX12345')))
                
                # construct the Sherlock query 
                query = [{
                    'name': name,
                    'ra': alert['candidate']['ra'],
                    'dec': alert['candidate']['dec']
                    }]
                # The original Sherlock driver uses json input/output
                # but we don't really need to do that here
                response = run_sherlock(request=query, json_output=False)
                print ("Sherlock response: " + json.dumps(response, indent=2))

                # Add new attributes to alert and republish
                alert.update(response[name])
                p.produce(output_topic, value=json.dumps(alert))
                print ("Produced output on " + output_topic)
            else:
                print ("Error: " + str(msg))
            n += 1
    finally:
        c.close()
        p.flush()
    return

if __name__ == '__main__':
    try:
        maxmsgs = int(sys.argv[1])
    except IndexError:
        maxmsgs = 10
    input_topic = 'dev_sherlock_test_input'
    output_topic = 'dev_sherlock_test_output'
    wrapper(input_topic, output_topic, maxmsgs)

