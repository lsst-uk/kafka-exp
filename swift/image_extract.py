# Extract ZTF images from a Kafka source and write to a Swift object store
#
from confluent_kafka import Consumer, KafkaError
import fastavro
from io import BytesIO
import os
import logging
from swiftclient.service import SwiftError, SwiftService, SwiftUploadObject
from swiftclient.multithreading import OutputManager

settings = {
    'bootstrap.servers': '192.41.108.22:9092',
    'group.id': 'testcongroup',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

_opts = {
    'object_uu_threads': 3,
    "auth_version": os.environ.get('ST_AUTH_VERSION'),  # Should be '3'
    "os_username": os.environ.get('OS_USERNAME'),
    "os_password": os.environ.get('OS_PASSWORD'),
    "os_project_name": os.environ.get('OS_PROJECT_NAME'),
    "os_project_domain_name": os.environ.get('OS_PROJECT_DOMAIN_NAME'),
    "os_auth_url": os.environ.get('OS_AUTH_URL')
}

logging.basicConfig(level=logging.ERROR)
logging.getLogger("requests").setLevel(logging.CRITICAL)
logging.getLogger("swiftclient").setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)

maxmsgs = 1000
n = 0
topic = 'ztf_20200103_programid1'

def make_upload_object(stamp_dict):
    """Given a stamp dict that follows the cutout schema,
       create a corresponding SwiftUploadObject.
    """
    try:
        obj = SwiftUploadObject(
                source=BytesIO(stamp_dict['stampData']),
                object_name=stamp_dict['fileName'])
    except TypeError:
        print('%% Cannot get stamp\n')
    return obj


print ("Start")

with SwiftService(options=_opts) as swift, OutputManager() as out_manager:
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
                    objs = []
                    objs.append(make_upload_object(alert.get('cutoutDifference')))
                    objs.append(make_upload_object(alert.get('cutoutTemplate')))
                    objs.append(make_upload_object(alert.get('cutoutScience')))
                    container = topic
                    for r in swift.upload(container, objs):
                        if r['success']:
                            if 'object' in r:
                                logger.debug(
                                        "uploaded object: %s" % (r['object']))
                            elif 'for_object' in r:
                                logger.debug(
                                        'uploaded %s segment %s' % (r['for_object'],
                                           r['segment_index']))
                        else:
                            error = r['error']
                            if r['action'] == "create_container":
                                logger.warning(
                                    'Warning: failed to create container '
                                    "'%s'%s", container, error)
                            elif r['action'] == "upload_object":
                                logger.error(
                                    "Failed to upload object %s to container %s: %s" %
                                    (r['object'], container, error))
                            else:
                                logger.error("%s" % error)
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
    except SwiftError as e:
        logger.error(e.value)
    finally:
        c.close()

print ("Got %d messages" % (n))

