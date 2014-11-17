from boto import connect_sqs
from boto.sqs.message import RawMessage
from tilequeue.tile import CoordMessage
from tilequeue.tile import deserialize_coord
from tilequeue.tile import serialize_coord


class SqsQueue(object):

    def __init__(self, sqs_queue, redis_client):
        self.sqs_queue = sqs_queue
        self.redis_client = redis_client
        self.inflight_key = "tilequeue.in-flight"

    def enqueue(self, coord):
        payload = serialize_coord(coord)
        message = RawMessage()
        message.set_body(payload)
        if self._inflight(coord):
            self._add_to_flight(coord)
            self.sqs_queue.write(message)

    def _write_batch(self, coords):
        assert len(coords) <= 10
        msg_tuples = [(str(i), serialize_coord(coord), 0)
                      for i, coord in enumerate(coords)]
        self.sqs_queue.write_batch(msg_tuples)

    def _inflight(self, coord):
        return self.redis_client.sismember(self.inflight_key,
                                           serialize_coord(coord))

    def _add_to_flight(self, coord):
        self.redis_client.sadd(self.inflight_key,
                               serialize_coord(coord))

    def enqueue_batch(self, coords):
        buffer = []
        n = 0
        for coord in coords:
            if self._inflight(coord):
                self._add_to_flight(coord)
                buffer.append(coord)
            if len(buffer) == 10:
                self._write_batch(buffer)
                del buffer[:]
            n += 1
        if buffer:
            self._write_batch(buffer)
        return n

    def read(self, max_to_read=1, timeout_seconds=20):
        coord_messages = []
        messages = self.sqs_queue.get_messages(num_messages=max_to_read)
        if not messages:
            message = self.sqs_queue.read(wait_time_seconds=timeout_seconds)
            if message is None:
                return []
            messages = [message]
        for message in messages:
            data = message.get_body()
            coord = deserialize_coord(data)
            if coord is None:
                # log?
                continue
            coord_message = CoordMessage(coord, message)
            coord_messages.append(coord_message)
        return coord_messages

    def job_done(self, message):
        self.redis_client.srem(self.inflight_key, message.get_body())
        self.sqs_queue.delete_message(message)

    def jobs_done(self, messages):
        payloads = []
        for message in messages:
            payloads.append(message.get_body())
        self.redis_client.srem(self.inflight_key, *payloads)
        self.sqs_queue.delete_message_batch(messages)

    def clear(self):
        self.redis_client.delete(self.inflight_key)
        n = 0
        while True:
            msgs = self.sqs_queue.get_messages(10)
            if not msgs:
                break
            self.sqs_queue.delete_message_batch(msgs)
            n += len(msgs)
        return n

    def close(self):
        pass


def make_sqs_queue(queue_name, cfg=None):
    # this doesn't actually create a queue in aws, it just creates a python
    # queue object
    conn = connect_sqs(cfg.aws_access_key_id, cfg.aws_secret_access_key)
    queue = conn.get_queue(queue_name)
    assert queue is not None, \
        'Could not get sqs queue with name: %s' % queue_name
    queue.set_message_class(RawMessage)
    from redis import StrictRedis
    redis_client = StrictRedis(cfg.redis_host, cfg.redis_port, cfg.redis_db)
    return SqsQueue(queue, redis_client)
