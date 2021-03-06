# write objects to a conatiner

from time import time
import random
from swiftclient.service import SwiftError, SwiftService, SwiftUploadObject
from swiftclient.multithreading import OutputManager
from os import environ 
from io import BytesIO

# number of objects to write
n = 1000

# size of object to write in KB
size = 15

# data type: 0=zero, 1=same random bytes, 2=different random bytes
d_type = 0

# container prefix
c_prefix = "benchmark_"

# object prefix
prefix = "iotest_"

_opts = {
    'object_dd_threads': 1,
    'object_uu_threads': 1,
    'container_threads': 10,
    'segment_threads': 10,
    "auth_version": environ.get('ST_AUTH_VERSION'),  # Should be '3'
    "os_username": environ.get('OS_USERNAME'),
    "os_password": environ.get('OS_PASSWORD'),
    "os_project_name": environ.get('OS_PROJECT_NAME'),
    "os_project_domain_name": environ.get('OS_PROJECT_DOMAIN_NAME'),
    "os_auth_url": environ.get('OS_AUTH_URL')
}

if d_type == 0:
    data = bytearray(size*1024)
if d_type == 1:
    data = bytearray(random.getrandbits(8) for _ in range(size*1024))

container = c_prefix+str(size)+'_'+str(d_type)

with SwiftService(options=_opts) as swift:
    start = time()
    for i in range(n):
        if d_type == 2:
            data = bytearray(random.getrandbits(8) for _ in range(size*1024))
        objs = []
        obj = SwiftUploadObject(
            source=BytesIO(data),
            object_name=prefix+str(i))
        objs.append(obj)
        for r in swift.upload(container, objs):
            pass
#            if r['success']:
#                if 'object' in r:
#                    print("uploaded object: %s" % (r['object']))
#            else:
#                error = r['error']
#                if r['action'] == "create_container":
#                    print(
#                        'Warning: failed to create container '
#                        "'%s'%s", container, error)
#                elif r['action'] == "upload_object":
#                    print(
#                        "Failed to upload object %s to container %s: %s" %
#                        (r['object'], container, error))
#                else:
#                    print("%s" % error)
    end = time()

    print("Wrote {:d} files of {:d}K in {:.2f} s".format(n, size, (end-start)))
    print("{:.2f} files/s at {:.2f} MB/s".format(n/(end-start), (n*size/(end-start)/1024)))

