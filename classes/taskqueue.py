from time import time


class TaskQueue:

    def __init__(self, timeout):
        self._timeout = timeout
        self._queue = {}

    def __str__(self):
        return self._queue

    def add(self, queue_name, data, data_len, uuid):
        if queue_name in self._queue:
            self._queue[queue_name].update({uuid: {"data": data, "data_len": data_len, "timestamp": 0}})
        else:
            self._queue.update({queue_name: {uuid: {"data": data, "data_len": data_len, "timestamp": 0}}})
        return uuid

    def get(self, queue_name):
        queue = self._queue.get(queue_name)
        cur_time = time()
        if queue:
            for qid in queue:
                if cur_time - int(queue[qid]["timestamp"]) >= self._timeout:
                    queue[qid]["timestamp"] = cur_time
                    result = queue[qid].copy()
                    result.update({"task_id": qid})
                    result.pop("timestamp")
                    return result

    def ack(self, queue_name, queue_id):
        if self.in_queue(queue_name, queue_id):
            return self._queue.get(queue_name).pop(queue_id)

    def in_queue(self, queue_name, queue_id):
        return self._queue.get(queue_name).get(queue_id)
