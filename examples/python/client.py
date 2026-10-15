# examples/python/client.py
import urllib.request
import urllib.error
import json
import time

PROXY_URL = "http://127.0.0.1:8000/pay"

print("=== Sending 10 Payment Requests Through FaultWire ===")

for i in range(1, 11):
    payload = json.dumps({"user": "alice", "amount": i * 100}).encode("utf-8")
    req = urllib.request.Request(
        PROXY_URL, 
        data=payload, 
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    start = time.time()
    try:
        # Client expects fast responses (300ms timeout)
        with urllib.request.urlopen(req, timeout=0.3) as resp:
            body = resp.read().decode("utf-8")
            elapsed = int((time.time() - start) * 1000)
            print(f"Payment #{i:02d} | {elapsed:4d}ms | SUCCESS   | Server replied: {body}")

    except urllib.error.HTTPError as e:
        elapsed = int((time.time() - start) * 1000)
        print(f"Payment #{i:02d} | {elapsed:4d}ms | HTTP {e.code}  | Server rejected corrupted packet!")

    except Exception as e:
        elapsed = int((time.time() - start) * 1000)
        print(f"Payment #{i:02d} | {elapsed:4d}ms | FAILED    | Network Failure: {type(e).__name__}: {e}")