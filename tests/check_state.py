"""
Script nhỏ để kiểm tra nhanh endpoint `/state` của coordinator local.

Dùng khi cụm đang chạy tại `127.0.0.1:9000` và cần xem JSON state hiện tại mà không mở dashboard.
"""

import requests
import json
import time

try:
    r = requests.get("http://127.0.0.1:9000/state", timeout=3)
    if r.status_code == 200:
        print(json.dumps(r.json(), indent=2))
    else:
        print(f"Status code: {r.status_code}")
except Exception as e:
    print(f"Error: {e}")
