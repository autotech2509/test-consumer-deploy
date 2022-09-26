from http.server import BaseHTTPRequestHandler
import threading


def print_hello_infiniti():
    while True:
        print("hello")


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        x = threading.Thread(target=print_hello_infiniti);
        x.start()
        return "oke"