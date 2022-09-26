from http.server import BaseHTTPRequestHandler
import threading
from os.path import join, dirname, abspath

from confluent_kafka import Consumer
import json
import ccloud_lib
dir = dirname(abspath(__file__))


def start_consumer():
    print("enter consumer success")
    consumer = None

    try:
        conf = ccloud_lib.read_ccloud_config(join(dir, '..', 'config', 'python.config'))
        consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
        consumer_conf['group.id'] = 'python_example_group_1'
        consumer_conf['auto.offset.reset'] = 'earliest'
        consumer = Consumer(consumer_conf)
    except Exception as er:
        print("{}".format(er))
        return

    print("Constructor consumer success")

    # Subscribe to topic
    consumer.subscribe(['test1'])

    # Process messages
    total_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                count = data['count']
                total_count += count
                print("Consumed record with key {} and value {}, \
                      and updated total count to {}"
                      .format(record_key, record_value, total_count))
    except KeyboardInterrupt:
        print("consumer stop by key")
    finally:
        # Leave group and commit final offsets
        print("consumer close")
        consumer.close()


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200, message="oke")
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        x = threading.Thread(target=start_consumer)
        x.start()
        return


if __name__ == '__main__':
    start_consumer()
