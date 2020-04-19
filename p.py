import time
import sys
import stomp
import os
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED
executor = ThreadPoolExecutor(max_workers=5)
from queue import Queue
import signal
import threading

class SendMsg:
    def __init__(self):
        self.receipt_dict = {}
        self.conn = stomp.Connection([('127.0.0.1', 61613)])
        self.conn.set_listener('a', SendListener(self.receipt_dict))
        # self.conn.set_listener('b', SendListener(self.receipt_dict))
        self.conn.connect('admin', 'password', wait=True)

    def test_send(self, receipt):
        for t in range(10):
            if self.receipt_dict[receipt]:
                print('no recep %d' % t)
                time.sleep(0.2)
            else:
                self.receipt_dict.pop(receipt)
                return 1
        return 0

    def sendmes(self, body, destination):
        receipt = str(id(self.sendmes))
        self.conn.send(body=body, destination=destination, receipt=receipt)
        self.receipt_dict[receipt] = 1
        if self.test_send(receipt):
            print('send right')
        else:
            print('send fail')
        #self.conn.disconnect()

    def __del__(self):
        print('print del')
        # self.conn.disconnect()


class SendListener(stomp.ConnectionListener):
    def __init__(self, receipt_dict):
        self.receipt_dict = receipt_dict

    def on_connecting(self, host_and_port):
        """
        :param (str,int) host_and_port:
        """
        print('on_connecting %s %s' % host_and_port)

    def on_connected(self, headers, body):
        """
        :param dict headers:
        :param body:
        """
        print('on_connected %s %s' % (headers, body))

    def on_disconnected(self):
        print('on_disconnected')

    def on_heartbeat_timeout(self):
        print('on_heartbeat_timeout')

    def on_before_message(self, headers, body):
        """
        :param dict headers:
        :param body:
        """
        print('on_before_message %s %s' % (headers, body))
        return headers, body

    def on_message(self, headers, body):
        print('on_message %s %s' % (headers, body))

    def on_receipt(self, headers, body):
        """
        :param dict headers:
        :param body:
        """
        print('on_receipt %s %s' % (headers, body))
        receipt = headers['receipt-id']
        self.receipt_dict[receipt] = 0
        # for k,v in self.receipt_dict.items():
        #     print(k, v)

    def on_error(self, headers, body):
        """
        :param dict headers:
        :param body:
        """
        print('on_error %s %s' % (headers, body))

    def on_send(self, frame):
        """
        :param Frame frame:
        """
        print('on_send %s %s %s' % (frame.cmd, frame.headers, frame.body))

    def on_heartbeat(self):
        print('on_heartbeat')

def test():
    sender = SendMsg()
    for i in range(20):
        sender.sendmes(body='test1-' + str(i), destination='/queue/test1')
        #sender.sendmes(body='test1-' + str(i), destination='/queue/test2')
        #time.sleep(2)
        print('******' * 3)


if __name__ == '__main__':
    task_lisst = []
    for i in range(200):
        t1 = threading.Thread(target=test, args=())
        t1.setDaemon(True)
        t1.start()
        task_lisst.append(t1)
    for t1 in task_lisst:
        t1.join()
    while True:
        pass
