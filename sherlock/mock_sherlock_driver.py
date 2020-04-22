# Mock Sherlock driver
# Based on lasair/src/sherlock_web_wrapper/server/sherlock_driver.py
# Takes a list of name/ra/dec as input, adds ra and dec and returns the result in the 'sum' attribute
# Returns empty output on error

import sys
import json
from time import sleep

# Time to sleep per record in ms
delay = 20

# Takes either a json string or dict as input. 
# Returns json output by default or a dict if json_output=False.
# Compatible with web version if called with a single arg.
def run_sherlock(json_request=None, request=None, json_output=True):
    if request is None:
        try:
            request = json.loads(json_request)
        except:
            return {}
    response = {}
    for obj in request:
        try:
            name = obj['name']
            ra = obj['ra']
            dec = obj['dec']
        except:
            continue
        response[name] = {}
        response[name]['sum'] = float(ra) + float(dec)
        sleep(delay/1000.0)
    if json_output:
        return json.dumps(response, indent=2)
    return response


if __name__ == '__main__':
    json_request = sys.stdin.read()
    json_response = run_sherlock(json_request)
    print(json_response)
