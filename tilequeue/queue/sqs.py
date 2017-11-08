from tilequeue.queue import MessageHandle
from tilequeue.utils import grouper


class SqsQueue(object):

    def __init__(self, sqs_client, queue_url, read_size,
                 recv_wait_time_seconds):
        self.sqs_client = sqs_client
        self.queue_url = queue_url
        self.read_size = read_size
        self.recv_wait_time_seconds = recv_wait_time_seconds

    def enqueue(self, payload):
        return self.sqs_client.send(
            QueueUrl=self.queue_url,
            MessageBody=payload,
        )

    def enqueue_batch(self, payloads):
        # sqs can only send 10 messages at once
        for payloads_chunk in grouper(payloads, 10):
            msgs = []
            for i, payload in enumerate(payloads_chunk):
                msg_id = str(i)
                msg = dict(
                    Id=msg_id,
                    MessageBody=payload,
                )
                msgs.append(msg)
            resp = self.sqs_client.send_message_batch(msgs)
            if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise Exception('Invalid status code from sqs: %s' %
                                resp['ResponseMetadata']['HTTPStatusCode'])
            failed_messages = resp.get('Failed')
            if failed_messages:
                # TODO maybe retry failed messages if not sender's fault? up to
                # a certain maximum number of attempts?
                # http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#SQS.Client.send_message_batch # noqa
                raise Exception('Messages failed to send to sqs: %s' %
                                len(failed_messages))

    def read(self):
        msg_handles = []
        resp = self.sqs_client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=self.read_size,
            AttributeNames=('SentTimestamp',),
            WaitTimeSeconds=self.recv_wait_time_seconds,
        )
        if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception('Invalid status code from sqs: %s' %
                            resp['ResponseMetadata']['HTTPStatusCode'])

        sqs_messages = resp.get('Messages')
        if not sqs_messages:
            return None
        for sqs_message in sqs_messages:
            payload = sqs_message['Body']
            try:
                timestamp = float(sqs_message['Attributes']['SentTimestamp'])
            except (TypeError, ValueError):
                timestamp = None
            sqs_handle = sqs_message['ReceiptHandle']

            metadata = dict(timestamp=timestamp)
            msg_handle = MessageHandle(sqs_handle, payload, metadata)
            msg_handles.append(msg_handle)

        return msg_handles

    def job_done(self, msg_handle):
        self.sqs_client.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=msg_handle.handle,
        )

    def clear(self):
        n = 0
        while True:
            msgs = self.read()
            if not msgs:
                break
            for msg in msgs:
                self.job_done(msg)
            n += len(msgs)
        return n

    def close(self):
        pass


def make_sqs_queue(name, region):
    import boto3
    sqs_client = boto3.client('sqs', region_name=region)
    resp = sqs_client.get_queue_url(QueueName=name)
    assert resp['ResponseMetadata']['HTTPStatusCode'] == 200, \
        'Failed to get queue url for: %s' % name
    queue_url = resp['QueueUrl']
    read_size = 10
    recv_wait_time_seconds = 20
    return SqsQueue(sqs_client, queue_url, read_size, recv_wait_time_seconds)
