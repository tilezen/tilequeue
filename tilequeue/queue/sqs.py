from boto import connect_sqs
from boto.sqs.message import RawMessage
from tilequeue.tile import CoordMessage
from tilequeue.tile import deserialize_coord
from tilequeue.tile import serialize_coord
from redis import StrictRedis


class SqsQueue(object):

    def __init__(self, sqs_queue, redis_client):
        self.sqs_queue = sqs_queue
        self.redis_client = redis_client
        self.inflight_key = "tilequeue.in-flight"

    def enqueue(self, coord):
        payload = serialize_coord(coord)
        message = RawMessage()
        message.set_body(payload)
        if not self._inflight(coord):
            self._add_to_flight(coord)
            self.sqs_queue.write(message)

    def _write_batch(self, coords):
        assert len(coords) <= 10
        values = []
        msg_tuples = []

        for i, coord in enumerate(coords):
            msg_tuples.append((str(i), serialize_coord(coord), 0))
            values.append(serialize_coord(coord))

        self.redis_client.sadd(self.inflight_key, *values)
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
            if not self._inflight(coord):
                buffer.append(coord)
            if len(buffer) == 10:
                self._write_batch(buffer)
                del buffer[:]
            n += 1
        if buffer:
            self._write_batch(buffer)
        return n

    def read(self, max_to_read=1):
        coord_messages = []
        messages = self.sqs_queue.get_messages(num_messages=max_to_read,
                                               attributes=["SentTimestamp"])
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


def get_sqs_queue(cfg=None):
    conn = connect_sqs(cfg.aws_access_key_id, cfg.aws_secret_access_key)
    queue = conn.get_queue(cfg.queue_name)
    assert queue is not None, \
        'Could not get sqs queue with name: %s' % cfg.queue_name
    queue.set_message_class(RawMessage)
    redis_client = StrictRedis(cfg.redis_host, cfg.redis_port, cfg.redis_db)
    return SqsQueue(queue, redis_client)
