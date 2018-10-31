from re import compile
import socket


class SocketCmdServer:

    def __init__(self, ip, port):
        self._ip = ip
        self._port = port
        self._cmd_patterns = self.prepare_patterns()

    def prepare_patterns(self):
        # TODO: maybe make static or class method and store patterns as class attributes
        cmd_patterns = ['(?P<cmd_type>ADD) (?P<queue_name>\S+) (?P<data_len>\d+) (?P<data>.+)',
                        '(?P<cmd_type>ACK) (?P<queue_name>\S+) (?P<task_id>.+)',
                        '(?P<cmd_type>GET) (?P<queue_name>\S+)',
                        '(?P<cmd_type>IN) (?P<queue_name>\S+) (?P<task_id>.+)',
                        '(?P<cmd_type>SAVE)']
        for idx in range(len(cmd_patterns)):
            cmd_patterns[idx] = compile(bytes(cmd_patterns[idx], 'utf8'))
        return cmd_patterns

    def start_polling(self, callback):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self._ip, self._port))
            s.listen(10)
            while True:
                conn, addr = s.accept()
                data = conn.recv(1024)
                if not data: break
                cmd = self._parse_cmd(data)
                if cmd:
                    data_len = cmd.get('data_len')
                    if data_len:
                        data_len = int(data_len.decode())
                        cmd['data'] += self._recieve(conn, data_len - len(cmd['data']))
                    ans = callback(cmd)
                    if ans:
                        conn.sendall(ans)
                else:
                    conn.send(bytes("ERROR", 'utf8'))
                conn.close()

    def _parse_cmd(self, data):
        for cmd_pattern in self._cmd_patterns:
            cmd = cmd_pattern.match(data)
            if cmd:
                return cmd.groupdict()

    def _recieve(self, conn, data_len):
        data = bytes()
        while len(data) < data_len:
            chunk = conn.recv(data_len - len(data))
            if chunk == '':
                raise RuntimeError('Broken')
            data += chunk
        return data
