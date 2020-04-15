import sys
import json
from time import sleep

def run_sherlock(json_request):
    reqs = json.loads(json_request)

    response = {}
    for obj in reqs:
        name = obj['name']
        response[name] = {}
        response[name]['ra'] = obj['ra']
        response[name]['dec'] = obj['dec']
        response[name]['sum'] = float(obj['ra']) + float(obj['dec'])
        sleep(.020)

    return json.dumps(response, indent=2)


if __name__ == '__main__':
    json_request = sys.stdin.read()
    json_response = run_sherlock(json_request)
    print(json_response)
