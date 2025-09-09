import sys
import json

import requests
import time


def token():
    response = requests.post(f"http://{sys.argv[1]}/login/", json={"username": "leonardo", "password": "subemelaradio"})
    return response.json()["token"]

def main():
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <backend_ip:port>")
        sys.exit(1)

    try:
        t = token()
    except Exception as e:
        print(f"Error fetching authenticating: {e}")
        sys.exit(1)

    while True:
        print(f"{json.dumps(requests.get(f'http://{sys.argv[1]}/devices/trk-18608d12c8446943ffbf2df9', headers={"Authorization": f"Bearer {t}"}).json(), indent=2)}")
        time.sleep(1)
if __name__ == "__main__":
    main()
