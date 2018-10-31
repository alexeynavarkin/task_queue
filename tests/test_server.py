from unittest import TestCase
from unittest.mock import patch
from os import remove

import time
import socket
import subprocess

from classes import TaskQueue

# class TaskQueueBaseTest(TestCase):
#     @patch('server.uuid4')
#     def test_timeout(self, patched_uuid4):
#         patched_uuid4().bytes = 'TEST_UUID'
#         task_queue = TaskQueue(timeout=1)
#
#         task_queue.add('first_queue', "Here is the first task!")
#         self.assertEqual(
#             {'data': 'Here is the first task!', 'task_id': 'TEST_UUID'},
#             task_queue.get('first_queue'))
#         self.assertEqual(
#             None,
#             task_queue.get('first_queue'))
#         time.sleep(1)
#         self.assertEqual(
#             {'data': 'Here is the first task!', 'task_id': 'TEST_UUID'},
#             task_queue.get('first_queue'))
#
#     @patch('server.uuid4')
#     def test_order(self, patched_uuid4):
#         task_queue = TaskQueue(timeout=1)
#         patched_uuid4().bytes = "TEST_UUID_1"
#         task_queue.add('first_queue', "Here is the first task!")
#         patched_uuid4().bytes = "TEST_UUID_2"
#         task_queue.add('first_queue', "Here is the second task!")
#
#         self.assertEqual(
#             {'data': 'Here is the first task!', 'task_id': 'TEST_UUID_1'},
#             task_queue.get('first_queue'), "Check for first item in queue.")
#         self.assertEqual(
#             {'data': 'Here is the second task!', 'task_id': 'TEST_UUID_2'},
#             task_queue.get('first_queue'), "Check for second item in queue.")
#
#
#     @patch('server.uuid4')
#     def test_ack(self, patched_uuid4):
#         task_queue = TaskQueue(timeout=1)
#         patched_uuid4().bytes = "TEST_UUID_1"
#         task_queue.add('first_queue', "Here is the first task!")
#         task_queue.ack('first_queue', 'TEST_UUID_1')
#         self.assertEqual(task_queue.get('first_queue'),
#                          None)


class ServerBaseTest(TestCase):
    def setUp(self):
        self.server = subprocess.Popen(['python3', 'server.py'])
        # даем серверу время на запуск
        time.sleep(0.5)
        self.remove_bkp()

    def tearDown(self):
        self.server.terminate()
        self.server.wait()
        self.remove_bkp()

    def remove_bkp(self):
        try:
            remove("./journal.bkp")
        except Exception:
            pass
        try:
            remove("./queue.bkp")
        except Exception:
            pass

    def send(self, command):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('127.0.0.1', 5555))
        s.send(command)
        data = s.recv(1000000)
        s.close()
        return data

    def test_base_scenario(self):
        task_id = self.send(b'ADD 1 5 12345')
        self.assertEqual(b'YES', self.send(b'IN 1 ' + task_id))

        self.assertEqual(task_id + b' 5 12345', self.send(b'GET 1'))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + task_id))
        self.assertEqual(b'YES', self.send(b'ACK 1 ' + task_id))
        self.assertEqual(b'NO', self.send(b'ACK 1 ' + task_id))
        self.assertEqual(b'NO', self.send(b'IN 1 ' + task_id))

    def test_two_tasks(self):
        first_task_id = self.send(b'ADD 1 5 12345')
        second_task_id = self.send(b'ADD 1 5 12345')
        self.assertEqual(b'YES', self.send(b'IN 1 ' + first_task_id))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + second_task_id))

        self.assertEqual(first_task_id + b' 5 12345', self.send(b'GET 1'))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + first_task_id))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + second_task_id))
        self.assertEqual(second_task_id + b' 5 12345', self.send(b'GET 1'))

        self.assertEqual(b'YES', self.send(b'ACK 1 ' + second_task_id))
        self.assertEqual(b'NO', self.send(b'ACK 1 ' + second_task_id))

    def test_long_input(self):
        data = '12345' * 1000
        data = '{} {}'.format(len(data), data)
        data = data.encode('utf')
        task_id = self.send(b'ADD 1 ' + data)
        self.assertEqual(b'YES', self.send(b'IN 1 ' + task_id))
        self.assertEqual(task_id + b' ' + data, self.send(b'GET 1'))

    def test_wrong_command(self):
        self.assertEqual(b'ERROR', self.send(b'ADDD 1 5 12345'))


if __name__ == '__main__':
    unittest.main()
