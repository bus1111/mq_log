import re
import time
from threading import Thread
import paho.mqtt.client as mqtt

re_connected = re.compile(r"\d+: New client connected from .+ as (\S+)")
re_timeout = re.compile(r"\d+: Client (\S+) has exceeded timeout")
re_disconnected = re.compile(r"\d+: Client (\S+) closed its connection")


class LogReader(mqtt.Client):
    def __init__(self):
        super().__init__()
        self._client_id = 'log_reader'
        self.username_pw_set('admin', 'qwe')
        self.connect('localhost', port=1884)

    def on_connect(self, client, userdata, flags, rc):
        client.subscribe('$SYS/broker/log/#')

    def on_message(self, client, userdata, msg):
        msg = msg.payload.decode('utf-8')
        # print(msg)
        if re_connected.match(msg):
            user_id, device_id = re_connected.match(msg).group(1).split('/')
            print(f'{device_id} connected')
        elif re_timeout.match(msg):
            user_id, device_id = re_timeout.match(msg).group(1).split('/')
            print(f'{device_id} timed out')
        elif re_disconnected.match(msg):
            user_id, device_id = re_disconnected.match(msg).group(1).split('/')
            print(f'{device_id} disconnected')


class TimeoutTester(mqtt.Client):
    def __init__(self, username='vakio_user_1', password='123', device_id='1/1'):
        super().__init__(client_id=f'device_{device_id}')
        self.username_pw_set(username, password)
        self.device_id = device_id
        self.connect('localhost', port=1884, keepalive=5)

    def on_connect(self, client, userdata, flags, rc):
        self.subscribe(f'{self.device_id}/#')
    
    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            time.sleep(0.3)
            self.reconnect()

    def run_test_loop(self):
        start = time.time()
        while True:
            if (time.time() - start) > 2:
                # print('Forcing a timeout...')
                time.sleep(self._keepalive + 2)
                start = time.time()
            else:
                self.loop(timeout=3)


if __name__ == "__main__":
    log_reader = LogReader()
    tester = TimeoutTester()
    thread = Thread(target=tester.run_test_loop)
    thread.start()
    log_reader.loop_forever()