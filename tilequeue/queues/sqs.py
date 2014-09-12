from boto import connect_sqs
from boto.sqs.message import RawMessage
from tilequeue.tile import deserialize_tile
from tilequeue.tile import serialize_tile
from tilequeue.tile import TileMessage

class SqsQueue(object):

    def __init__(self, sqs_queue):
        self.sqs_queue = sqs_queue

    def enqueue_tile(self, tile):
        payload = serialize_tile(tile)
        message = RawMessage()
        message.set_body(payload)
        self.sqs_queue.write(message)

    def enqueue_tiles(self, tiles):
        # unclear if boto will handle submitting more than 10 at once
        # to be safe we do that here
        if len(tiles) <= 10:
            messages = []
            for i, tile in enumerate(tiles):
                msg_tuple = (str(i), serialize_tile(tile), 0)
                messages.append(msg_tuple)
            self.sqs_queue.write_batch(messages)
        else:
            self.enqueue_tiles(tiles[:10])
            self.enqueue_tiles(tiles[10:])

    def read_tiles(self, max_tiles=1, timeout_seconds=20):
        tile_messages = []
        messages = self.sqs_queue.get_messages(num_messages=max_tiles)
        if not messages:
            message = self.sqs_queue.read(wait_time_seconds=timeout_seconds)
            if message is None:
                return []
            messages = [message]
        for message in messages:
            data = message.get_body()
            tile = deserialize_tile(data)
            if tile is None:
                # log?
                continue
            tile_message = TileMessage(tile, message)
            tile_messages.append(tile_message)
        return tile_messages

    def job_done(self, message):
        self.sqs_queue.delete_message(message)

    def jobs_done(self, messages):
        self.sqs_queue.delete_message_batch(messages)

def make_sqs_queue(queue_name, aws_access_key_id, aws_secret_access_key):
    # this doesn't actually create a queue in aws, it just creates a python
    # queue object
    conn = connect_sqs(aws_access_key_id, aws_secret_access_key)
    queue = conn.get_queue(queue_name)
    assert queue is not None, 'Could not get sqs queue with name: %s' % queue_name
    return SqsQueue(queue)
