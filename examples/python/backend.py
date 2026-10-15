# examples/python/backend.py
from http.server import BaseHTTPRequestHandler, HTTPServer
import json

class PaymentBackend(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        raw_body = self.rfile.read(content_length)

        try:
            data = json.loads(raw_body.decode('utf-8'))
            user = data["user"]
            amount = data["amount"]

            print(f"[PYTHON BACKEND] Processed payment: User={user}, Amount=${amount}")

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            response = {"status": "approved", "user": user, "charged": amount}
            self.wfile.write(json.dumps(response).encode('utf-8'))

        except Exception as e:
            print(f"[PYTHON BACKEND] CRASH! Invalid payload received: {raw_body} | Error: {e}")
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'{"status": "error", "reason": "corrupted_payload"}')

    def log_message(self, format, *args):
        return # Keep terminal output clean

if __name__ == "__main__":
    server = HTTPServer(("127.0.0.1", 9000), PaymentBackend)
    print("[PYTHON BACKEND] Payment Server running on http://127.0.0.1:9000")
    server.serve_forever()