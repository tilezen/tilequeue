import re
import sys
import traceback
from collections import defaultdict
from datetime import datetime
from itertools import islice
from time import time

import boto3
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session

from tilequeue.tile import coord_marshall_int
from tilequeue.tile import create_coord


def format_stacktrace_one_line(exc_info=None):
    # exc_info is expected to be an exception tuple from sys.exc_info()
    if exc_info is None:
        exc_info = sys.exc_info()
    exc_type, exc_value, exc_traceback = exc_info
    exception_lines = traceback.format_exception(exc_type, exc_value,
                                                 exc_traceback)
    stacktrace = ' | '.join([x.replace('\n', '')
                             for x in exception_lines])
    return stacktrace


def grouper(iterable, n):
    """Yield n-length chunks of the iterable"""
    it = iter(iterable)
    while True:
        chunk = tuple(islice(it, n))
        if not chunk:
            return
        yield chunk


def parse_log_file(log_file):
    ip_pattern = r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
    # didn't match againts explicit date pattern, in case it changes
    date_pattern = r'\[([\d\w\s\/:]+)\]'
    tile_id_pattern = r'\/([\w]+)\/([\d]+)\/([\d]+)\/([\d]+)\.([\d\w]*)'

    log_pattern = r'%s - - %s "([\w]+) %s.*' % (
        ip_pattern, date_pattern, tile_id_pattern)

    tile_log_records = []
    for log_string in log_file:
        match = re.search(log_pattern, log_string)
        if match and len(match.groups()) == 8:
            tile_log_records.append(
                (match.group(1),
                 datetime.strptime(match.group(2), '%d/%B/%Y %H:%M:%S'),
                 coord_marshall_int(
                     create_coord(
                         match.group(6), match.group(7), match.group(5)))))

    return tile_log_records


def encode_utf8(x):
    if x is None:
        return None
    elif isinstance(x, unicode):
        return x.encode('utf-8')
    elif isinstance(x, dict):
        result = {}
        for k, v in x.items():
            if isinstance(k, unicode):
                k = k.encode('utf-8')
            result[k] = encode_utf8(v)
        return result
    elif isinstance(x, list):
        return map(encode_utf8, x)
    elif isinstance(x, tuple):
        return tuple(encode_utf8(list(x)))
    else:
        return x


class time_block(object):

    """Convenience to capture timing information"""

    def __init__(self, timing_state, key):
        # timing_state should be a dictionary
        self.timing_state = timing_state
        self.key = key

    def __enter__(self):
        self.start = time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        stop = time()
        duration_seconds = stop - self.start
        duration_millis = duration_seconds * 1000
        self.timing_state[self.key] = duration_millis


class CoordsByParent(object):

    def __init__(self, parent_zoom):
        self.parent_zoom = parent_zoom
        self.groups = defaultdict(list)

    def add(self, coord, *extra):
        data = coord
        if extra:
            data = (coord,) + extra

        # treat tiles as singletons below the parent zoom
        if coord.zoom < self.parent_zoom:
            self.groups[coord].append(data)

        else:
            # otherwise, group by the parent tile at the parent zoom.
            parent_coord = coord.zoomTo(self.parent_zoom).container()
            self.groups[parent_coord].append(data)

    def __iter__(self):
        return self.groups.iteritems()


def convert_seconds_to_millis(time_in_seconds):
    time_in_millis = int(time_in_seconds * 1000)
    return time_in_millis


class AwsSessionHelper:
    """ The AwsSessionHelper creates a auto-refreshable boto3 session object
        and allows for creating clients with those refreshable credentials.
    """

    def __init__(self, session_name, role_arn, region='us-east-1',
                 s3_role_session_duration_s=3600):
        """ session_name: str; The name of the session we are creating
            role_arn: str; The ARN of the role we are assuming with STS
            region: str; The region for the STS client to be created in
            s3_role_session_duration_s: int; the time that session is good for
        """
        self.role_arn = role_arn
        self.session_name = session_name
        self.region = region
        self.session_duration_seconds = s3_role_session_duration_s
        self.sts_client = boto3.client('sts')

        credentials = self._refresh()
        session_credentials = RefreshableCredentials.create_from_metadata(
            metadata=credentials,
            refresh_using=self._refresh,
            method='sts-assume-role'
        )
        aws_session = get_session()
        aws_session._credentials = session_credentials
        aws_session.set_config_variable('region', region)
        self.aws_session = boto3.Session(botocore_session=aws_session)

    def get_client(self, service):
        """ Returns boto3.client with the refreshable session

            service: str; String of what service to create a client for
            (e.g. 'sqs', 's3')
        """
        return self.aws_session.client(service)

    def get_session(self):
        """ Returns the raw refreshable aws session
        """
        return self.aws_session

    def _refresh(self):
        params = {
            'RoleArn': self.role_arn,
            'RoleSessionName': self.session_name,
            'DurationSeconds': self.session_duration_seconds,
        }

        response = self.sts_client.assume_role(**params).get('Credentials')
        credentials = {
            'access_key': response.get('AccessKeyId'),
            'secret_key': response.get('SecretAccessKey'),
            'token': response.get('SessionToken'),
            'expiry_time': response.get('Expiration').isoformat(),
        }
        return credentials
