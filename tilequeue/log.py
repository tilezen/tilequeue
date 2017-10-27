from enum import Enum
import json
import logging
import sys


def int_if_exact(x):
    try:
        i = int(x)
        return i if i == x else x
    except ValueError:
        # shouldn't practically happen, but prefer to just log the original
        # instead of explode
        return x


def make_coord_dict(coord):
    """helper function to make a dict from a coordinate for logging"""
    return dict(
        z=int_if_exact(coord.zoom),
        x=int_if_exact(coord.column),
        y=int_if_exact(coord.row),
    )


class LogLevel(Enum):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR


class LogCategory(Enum):
    PROCESS = 1
    LIFECYCLE = 2
    QUEUE_SIZES = 3
    RAWR_PROCESS = 4


def log_level_name(log_level):
    return log_level.name.lower()


def log_category_name(log_category):
    return log_category.name.lower()


class JsonTileProcessingLogger(object):

    def __init__(self, logger):
        self.logger = logger

    def log(self, log_level, log_category, msg, exception,
            formatted_stacktrace, coord):
        try:
            log_level_str = log_level_name(log_level)
            logging_log_level = log_level.value
        except Exception:
            sys.stderr.write('ERROR: code error: invalid log level: %s\n' %
                             log_level)
            log_level_str = log_level_name(LogLevel.ERROR)
            logging_log_level = logging.ERROR
        try:
            log_category_str = log_category_name(log_category)
        except Exception:
            sys.stderr.write('ERROR: code error: invalid log category: %s\n' %
                             log_category)
            log_category_str = log_category_name(LogCategory.PROCESS)

        json_obj = dict(
            category=log_category_str,
            type=log_level_str,
            msg=msg,
        )
        if exception:
            json_obj['exception'] = str(exception)
        if formatted_stacktrace:
            json_obj['stacktrace'] = formatted_stacktrace,
        if coord:
            json_obj['coord'] = make_coord_dict(coord)
        json_str = json.dumps(json_obj)
        self.logger.log(logging_log_level, json_str)

    def error(self, msg, exception, formatted_stacktrace, coord=None):
        self.log(LogLevel.ERROR, LogCategory.PROCESS, msg, exception,
                 formatted_stacktrace, coord)

    def log_processed_coord(self, coord_proc_data):
        json_obj = dict(
            category=log_category_name(LogCategory.PROCESS),
            type=log_level_name(LogLevel.INFO),
            coord=make_coord_dict(coord_proc_data.coord),
            timing=coord_proc_data.timing,
            size=coord_proc_data.size,
            storage=coord_proc_data.store_info,
        )
        json_str = json.dumps(json_obj)
        self.logger.info(json_str)

    def lifecycle(self, msg):
        self.log(
            LogLevel.INFO, LogCategory.LIFECYCLE, msg, None, None, None)

    def log_queue_sizes(self, queue_info):
        sizes = {}
        for queue, queue_name in queue_info:
            size = dict(size=queue.qsize())
            if queue.empty():
                size['empty'] = True
            if queue.full():
                size['full'] = True
            sizes[queue_name] = size
        json_obj = dict(
            category=log_category_name(LogCategory.QUEUE_SIZES),
            type=log_level_name(LogLevel.INFO),
            sizes=sizes,
        )
        json_str = json.dumps(json_obj)
        self.logger.info(json_str)


class JsonRawrProcessingLogger(object):

    def __init__(self, logger):
        self.logger = logger

    def error(self, msg, exception, stacktrace, parent_coord):
        json_obj = dict(
            type=log_level_name(LogLevel.ERROR),
            category=log_category_name(LogCategory.RAWR_PROCESS),
            msg=msg,
            exception=str(exception),
            stacktrace=stacktrace,
        )
        if parent_coord:
            json_obj['coord'] = make_coord_dict(parent_coord)
        json_str = json.dumps(json_obj)
        self.logger.error(json_str)

    def processed(self, intersect_metrics, n_enqueued, n_inflight, timing,
                  parent_coord):
        json_obj = dict(
            type=log_level_name(LogLevel.INFO),
            category=log_category_name(LogCategory.RAWR_PROCESS),
            intersect=dict(
                toi=intersect_metrics['n_toi'],
                total=intersect_metrics['total'],
                hits=intersect_metrics['hits'],
                misses=intersect_metrics['misses'],
                cached=intersect_metrics['cached'],
            ),
            enqueued=n_enqueued,
            inflight=n_inflight,
            coord=make_coord_dict(parent_coord),
            time=timing,
        )
        json_str = json.dumps(json_obj)
        self.logger.info(json_str)

    def lifecycle(self, msg):
        json_obj = dict(
            type=log_level_name(LogLevel.INFO),
            category=log_category_name(LogCategory.LIFECYCLE),
            msg=msg,
        )
        json_str = json.dumps(json_obj)
        self.logger.info(json_str)
