from datetime import datetime
from tilequeue.queue import MessageHandle
from tilequeue.utils import grouper
import threading


class VisibilityState(object):

    def __init__(self, last, total):
        # the datetime when the message was last extended
        self.last = last
        # the total amount of time currently extended
        self.total = total


class VisibilityManager(object):

    def __init__(self, extend_secs, max_extend_secs, timeout_secs):
        self.extend_secs = extend_secs
        self.max_extend_secs = max_extend_secs
        self.timeout_secs = timeout_secs
        self.handle_state_map = {}
        self.lock = threading.Lock()

    def should_extend(self, handle, now=None):
        if now is None:
            now = datetime.now()
        with self.lock:
            state = self.handle_state_map.get(handle)
            if not state:
                return True
            if state.total + self.extend_secs > self.max_extend_secs:
                return False
            delta = now - state.last
            return delta.seconds > self.extend_secs

    def extend(self, handle, now=None):
        if now is None:
            now = datetime.now()
        with self.lock:
            state = self.handle_state_map.get(handle)
            if state:
                state.last = now
                state.total += self.extend_secs
            else:
                state = VisibilityState(now, self.extend_secs)
                self.handle_state_map[handle] = state

    def done(self, handle):
        try:
            with self.lock:
                del self.handle_state_map[handle]
        except KeyError:
            pass


class JobProgressException(Exception):

    def __init__(self, msg, cause, err_details):
        super(JobProgressException, self).__init__(
            msg + ', caused by ' + repr(cause))
        self.err_details = err_details


class SqsQueue(object):

    def __init__(self, sqs_client, queue_url, read_size,
                 recv_wait_time_seconds, visibility_mgr):
        self.sqs_client = sqs_client
        self.queue_url = queue_url
        self.read_size = read_size
        self.recv_wait_time_seconds = recv_wait_time_seconds
        self.visibility_mgr = visibility_mgr

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
            resp = self.sqs_client.send_message_batch(
                QueueUrl=self.queue_url,
                Entries=msgs,
            )
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
            VisibilityTimeout=self.visibility_mgr.timeout_secs,
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

    def job_done(self, handle):
        self.visibility_mgr.done(handle)
        self.sqs_client.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=handle,
        )

    def job_progress(self, handle):
        if self.visibility_mgr.should_extend(handle):
            self.visibility_mgr.extend(handle)

            try:
                self.sqs_client.change_message_visibility(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=handle,
                    VisibilityTimeout=self.visibility_mgr.extend_secs,
                )
            except Exception as e:
                visibility_state = self.visibility_mgr.lookup(handle)
                err_details = dict(
                    visibility=dict(
                        last=visibility_state.last.isoformat(),
                        total=visibility_state.total,
                        ))
                raise JobProgressException(
                    'update visibility timeout', e, err_details)

    def clear(self):
        n = 0
        while True:
            msgs = self.read()
            if not msgs:
                break
            for msg in msgs:
                self.job_done(msg.handle)
            n += len(msgs)
        return n

    def close(self):
        pass


def make_visibility_manager(extend_secs, max_extend_secs, timeout_secs):
    visibility_mgr = VisibilityManager(extend_secs, max_extend_secs,
                                       timeout_secs)
    return visibility_mgr


def make_sqs_queue(name, region, visibility_mgr):
    import boto3
    sqs_client = boto3.client('sqs', region_name=region)
    resp = sqs_client.get_queue_url(QueueName=name)
    assert resp['ResponseMetadata']['HTTPStatusCode'] == 200, \
        'Failed to get queue url for: %s' % name
    queue_url = resp['QueueUrl']
    read_size = 10
    recv_wait_time_seconds = 20
    return SqsQueue(sqs_client, queue_url, read_size, recv_wait_time_seconds,
                    visibility_mgr)
