from .socketserver import SocketCmdServer
from .taskqueue import TaskQueue
from uuid import uuid4
from re import compile
import pickle

# TODO: maybe CTRL+C exception
# def prompt_shutdown():
#     while True:
#         print('"CTRL+C" detected, shutdown server?', end=" ")
#         rep = input('(yes|no)')
#         if rep == 'yes':
#             return True
#         elif rep == 'no':
#            return False


class TaskQueueServer:

    def __init__(self, ip, port, path, timeout):
        self._path = path
        self._sock_server = SocketCmdServer(ip, int(port))
        self._queue = TaskQueue(int(timeout))

    def run(self):
        self.restore()
        self.restore_from_journal()
        self._sock_server.start_polling(self.run_cmd)

    def save(self):
        with open(self._path+'queue.bkp', 'wb') as f:
            pickle.dump(self._queue, f)
            self.wipe_journal()
        return True

    def restore(self):
        try:
            f = open(self._path+'queue.bkp', 'rb')
            bkp = pickle.load(f)
        except FileNotFoundError:
            print(f'No "queue.bkp" file found in "{self._path}".')
        except EOFError:
            print()
        else:
            f.close()
            self._queue = bkp
            print(f'Restored from "{self._path}db.bkp" successfully.')

    def restore_from_journal(self):
        try:
            f = open(self._path+'journal.bkp', 'r')
        except FileNotFoundError:
            print(f'No "journal.bkp" file found in "{self._path}".')
        else:
            print("Found journal file, trying to restore state.\nRestored commands:")
            pattern = compile('{.*?}')
            # match = pattern.findall(f.read()) #Not stream like but maybe safe
            # for cmd in match:
            #     cmd = eval(cmd)
            #     rep = self.run_cmd(cmd, False)
            #     print(f"Cmd: {cmd} -> Rep: {rep}")
            # f.close()
            idx = 0
            line = f.readline()
            while line:
                match = pattern.match(line)
                if match:
                    idx += 1
                    cmd = eval(match.group())
                    rep = self.run_cmd(cmd, False)
                    print(f"{idx}.Cmd: {cmd} -> Rep: {rep}")
                    line = f.readline()
            if idx:
                print("End restored commands.")
            else:
                print("Journal file empty.")

    def write_journal(self, cmd):
        with open(self._path+'journal.bkp', 'a') as f:
            f.write(str(cmd)+'\n')

    def wipe_journal(self):
        with open(self._path + 'journal.bkp', 'w') as f:
            print("Journal wiped.")

    def run_cmd(self, cmd, journal=True):
        if cmd['cmd_type'] == b'ADD':
            return self._run_add(cmd, journal)

        if cmd['cmd_type'] == b'GET':
            return self._run_get(cmd)

        if cmd['cmd_type'] == b'ACK':
            return self._run_ack(cmd, journal)

        if cmd['cmd_type'] == b'IN':
            return self._run_in(cmd)

        if cmd['cmd_type'] == b'SAVE':
            return self._run_save()

    def _run_add(self, cmd, journal):
        if not cmd.get('task_id'):
            cmd.update({"task_id": uuid4().bytes})
        if journal:
            self.write_journal(cmd)
        return self._queue.add(cmd['queue_name'], cmd['data'], cmd['data_len'], cmd['task_id'])

    def _run_get(self, cmd):
        result = self._queue.get(cmd['queue_name'])
        if result:
            result = result['task_id'] + b" " + result['data_len'] + b" " + result['data']
            return result
        return b'NONE'

    def _run_ack(self, cmd, journal):
        if journal:
            self.write_journal(cmd)
        if self._queue.ack(cmd['queue_name'], cmd['task_id']):
            return b'YES'
        else:
            return b'NO'

    def _run_in(self, cmd):
        if self._queue.in_queue(cmd['queue_name'], cmd['task_id']):
            return b'YES'
        else:
            return b'NO'

    def _run_save(self):
        if self.save():
            return b'OK'