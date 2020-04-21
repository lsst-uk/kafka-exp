# Mock Sherlock driver
# Based on lasair/src/sherlock_web_wrapper/server/sherlock_driver.py
# Takes a list of name/ra/dec as input, adds ra and dec and returns the result in the 'sum' attribute
# Returns empty output on error

import sys
import json
from time import sleep

# Time to sleep per record in ms
delay = 20

def run_sherlock(json_request):
    response = {}
    try:
        reqs = json.loads(json_request)
        for obj in reqs:
            try:
                name = obj['name']
                ra = obj['ra']
                dec = obj['dec']
            except:
                continue
            response[name] = {}
            response[name]['ra'] = ra
            response[name]['dec'] = dec
            response[name]['sum'] = float(ra) + float(dec)
            sleep(delay/1000.0)
    except:
        pass
    return json.dumps(response, indent=2)


if __name__ == '__main__':
    json_request = sys.stdin.read()
    json_response = run_sherlock(json_request)
    print(json_response)
