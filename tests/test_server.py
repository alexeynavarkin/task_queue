from unittest import TestCase
from os import remove

import time
import socket
import unittest
import subprocess

from classes import TaskQueue

class TaskQueueBaseTest(TestCase):
    def test_timeout(self,):
        task_queue = TaskQueue(timeout=1)

        task_queue.add('first_queue', "Here is the first task!", 23, 'TEST_UUID')
        self.assertEqual(
            {'data': 'Here is the first task!', 'data_len': 23, 'task_id': 'TEST_UUID'},
            task_queue.get('first_queue'))
        self.assertEqual(
            None,
            task_queue.get('first_queue'))
        time.sleep(1)
        self.assertEqual(
            {'data': 'Here is the first task!', 'data_len': 23, 'task_id': 'TEST_UUID'},
            task_queue.get('first_queue'))

    def test_order(self):
        task_queue = TaskQueue(timeout=1)
        task_queue.add('first_queue', "Here is the first task!", 23, "TEST_UUID_1")
        task_queue.add('first_queue', "Here is the second task!", 24, "TEST_UUID_2")

        self.assertEqual(
            {'data': 'Here is the first task!', 'data_len': 23, 'task_id': 'TEST_UUID_1'},
            task_queue.get('first_queue'), "Check for first item in queue.")
        self.assertEqual(
            {'data': 'Here is the second task!', 'data_len': 24,'task_id': 'TEST_UUID_2'},
            task_queue.get('first_queue'), "Check for second item in queue.")

    def test_ack(self):
        task_queue = TaskQueue(timeout=1)
        task_queue.add('first_queue', "Here is the first task!", 23, "TEST_UUID_1")
        task_queue.ack('first_queue', 'TEST_UUID_1')
        self.assertEqual(task_queue.get('first_queue'), None)


class ServerBaseTest(TestCase):
    def setUp(self):
        self.remove_bkp()
        self.server = subprocess.Popen(['python3', 'server.py'])
        # даем серверу время на запуск
        time.sleep(0.5)

    def tearDown(self):
        self.server.terminate()
        self.server.wait()
        self.remove_bkp()

    def remove_bkp(self):
        try:
            remove("./journal.bkp")
        except FileNotFoundError:
            pass
        try:
            remove("./queue.bkp")
        except FileNotFoundError:
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


class ServerJournalTest(TestCase):
    def remove_bkp(self):
        try:
            remove("./journal.bkp")
        except FileNotFoundError:
            pass
        try:
            remove("./queue.bkp")
        except FileNotFoundError:
            pass

    def run_server(self):
        self.server = subprocess.Popen(['python3', 'server.py'])
        # даем серверу время на запуск
        time.sleep(0.5)

    def stop_server(self):
        self.server.terminate()
        self.server.wait()

    def setUp(self):
        self.remove_bkp()
        self.run_server()

    def tearDown(self):
        self.stop_server()
        self.remove_bkp()

    def send(self, command):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('127.0.0.1', 5555))
        s.send(command)
        data = s.recv(1000000)
        s.close()
        return data

    def test_prepare(self):
        task_id = self.send(b'ADD 1 5 12345')
        self.assertEqual(b'YES', self.send(b'IN 1 ' + task_id))
        self.assertEqual(task_id + b' 5 12345', self.send(b'GET 1'))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + task_id))
        self.assertEqual(b'YES', self.send(b'ACK 1 ' + task_id))
        self.stop_server()
        self.run_server()
        self.assertEqual(b'NO', self.send(b'ACK 1 ' + task_id))
        self.assertEqual(b'NO', self.send(b'IN 1 ' + task_id))

    def test_two_tasks(self):
        first_task_id = self.send(b'ADD 1 5 12345')
        second_task_id = self.send(b'ADD 1 6 678910')
        self.assertEqual(b'YES', self.send(b'IN 1 ' + first_task_id))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + second_task_id))

        self.stop_server()
        self.run_server()

        self.assertEqual(first_task_id + b' 5 12345', self.send(b'GET 1'))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + first_task_id))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + second_task_id))
        self.assertEqual(second_task_id + b' 6 678910', self.send(b'GET 1'))

        self.stop_server()
        self.run_server()

        self.assertEqual(b'YES', self.send(b'ACK 1 ' + second_task_id))
        self.assertEqual(b'NO', self.send(b'ACK 1 ' + second_task_id))

    def test_long_input(self):
        data = '12345' * 1000
        data = '{} {}'.format(len(data), data)
        data = data.encode('utf')
        task_id = self.send(b'ADD 1 ' + data)
        self.assertEqual(b'YES', self.send(b'IN 1 ' + task_id))

        self.stop_server()
        self.run_server()

        self.assertEqual(task_id + b' ' + data, self.send(b'GET 1'))


class ServerSaveTest(TestCase):
    def remove_bkp(self):
        try:
            remove("./journal.bkp")
        except FileNotFoundError:
            pass
        try:
            remove("./queue.bkp")
        except FileNotFoundError:
            pass

    def run_server(self):
        self.server = subprocess.Popen(['python3', 'server.py'])
        # даем серверу время на запуск
        time.sleep(0.5)

    def stop_server(self):
        self.server.terminate()
        self.server.wait()

    def setUp(self):
        self.remove_bkp()
        self.run_server()

    def tearDown(self):
        self.stop_server()
        self.remove_bkp()

    def send(self, command):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('127.0.0.1', 5555))
        s.send(command)
        data = s.recv(1000000)
        s.close()
        return data

    def test_prepare(self):
        task_id = self.send(b'ADD 1 5 12345')
        self.assertEqual(b'YES', self.send(b'IN 1 ' + task_id))
        self.assertEqual(task_id + b' 5 12345', self.send(b'GET 1'))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + task_id))
        self.assertEqual(b'YES', self.send(b'ACK 1 ' + task_id))
        self.send(b'SAVE')
        self.stop_server()
        self.run_server()
        self.assertEqual(b'NO', self.send(b'ACK 1 ' + task_id))
        self.assertEqual(b'NO', self.send(b'IN 1 ' + task_id))

    def test_two_tasks(self):
        first_task_id = self.send(b'ADD 1 5 12345')
        second_task_id = self.send(b'ADD 1 6 678910')
        self.assertEqual(b'YES', self.send(b'IN 1 ' + first_task_id))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + second_task_id))
        self.send(b'SAVE')

        self.stop_server()
        self.run_server()

        self.assertEqual(first_task_id + b' 5 12345', self.send(b'GET 1'))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + first_task_id))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + second_task_id))
        self.assertEqual(second_task_id + b' 6 678910', self.send(b'GET 1'))
        self.send(b'SAVE')

        self.stop_server()
        self.run_server()

        self.assertEqual(b'YES', self.send(b'ACK 1 ' + second_task_id))
        self.assertEqual(b'NO', self.send(b'ACK 1 ' + second_task_id))

    def test_long_input(self):
        data = '12345' * 1000
        data = '{} {}'.format(len(data), data)
        data = data.encode('utf')
        task_id = self.send(b'ADD 1 ' + data)
        self.assertEqual(b'YES', self.send(b'IN 1 ' + task_id))
        self.send(b'SAVE')

        self.stop_server()
        self.run_server()

#         self.assertEqual(task_id + b' ' + data, self.send(b'GET 1'))



if __name__ == '__main__':
    unittest.main()
