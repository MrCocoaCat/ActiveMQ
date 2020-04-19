import time
import sys

import stomp


if __name__ == '__main__':
    conn = stomp.Connection([('127.0.0.1', 61613)])
    conn.set_listener('', stomp.PrintingListener())
    conn.connect('admin', 'password', wait=True)
    conn.subscribe(destination='/queue/test1', id=str(id(conn)), ack='auto')
    conn.subscribe(destination='/queue/test2', id=str(id(conn)), ack='auto')
    #conn.disconnect()
    while True:
        pass
