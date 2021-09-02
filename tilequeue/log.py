from enum import Enum
from tilequeue.utils import format_stacktrace_one_line
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
    RAWR_TILE = 5
    META_TILE = 6
    META_TILE_LOW_ZOOM = 7


class MsgType(Enum):
    INDIVIDUAL = 1
    PYRAMID = 2


def log_level_name(log_level):
    return log_level.name.lower()


def log_category_name(log_category):
    return log_category.name.lower()


def log_msg_type_name(log_msg_type):
    return log_msg_type.name.lower()


class JsonTileProcessingLogger(object):

    def __init__(self, logger):
        self.logger = logger

    def log(self, log_level, log_category, log_msg_type, msg, exception,
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

        if log_msg_type is not None:
            try:
                json_obj['msg_type'] = log_msg_type_name(log_msg_type)
            except Exception:
                sys.stderr.write(
                    'ERROR: code error: invalid log msg_type: %s\n' %
                    log_msg_type)

        if exception:
            json_obj['exception'] = str(exception)
        if formatted_stacktrace:
            json_obj['stacktrace'] = formatted_stacktrace,
        if coord:
            json_obj['coord'] = make_coord_dict(coord)
        json_str = json.dumps(json_obj)
        self.logger.log(logging_log_level, json_str)

    def error(self, msg, exception, formatted_stacktrace, coord=None):
        msg_type = None if coord is None else MsgType.INDIVIDUAL
        self.log(LogLevel.ERROR, LogCategory.PROCESS, msg_type, msg,
                 exception, formatted_stacktrace, coord)

    def log_processed_coord(self, coord_proc_data):
        json_obj = dict(
            category=log_category_name(LogCategory.PROCESS),
            type=log_level_name(LogLevel.INFO),
            msg_type=log_msg_type_name(MsgType.INDIVIDUAL),
            coord=make_coord_dict(coord_proc_data.coord),
            time=coord_proc_data.timing,
            size=coord_proc_data.size,
            storage=coord_proc_data.store_info,
        )
        json_str = json.dumps(json_obj)
        self.logger.info(json_str)

    def log_processed_pyramid(self, parent_tile,
                              start_time, stop_time):

        duration = stop_time - start_time

        json_obj = dict(
            type=log_level_name(LogLevel.INFO),
            category=log_category_name(LogCategory.PROCESS),
            msg_type=log_msg_type_name(MsgType.PYRAMID),
            coord=make_coord_dict(parent_tile),
            time=dict(
                start=start_time,
                stop=stop_time,
                duration=duration,
            ),
        )
        json_str = json.dumps(json_obj)
        self.logger.info(json_str)

    def lifecycle(self, msg):
        self.log(
            LogLevel.INFO, LogCategory.LIFECYCLE, None, msg, None, None, None)

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

    def _log_job_error(self, msg, exception, stacktrace, coord, parent_tile,
                       err_details):
        json_obj = dict(
            type=log_level_name(LogLevel.ERROR),
            category=log_category_name(LogCategory.PROCESS),
            msg=msg,
            coord=make_coord_dict(coord),
            exception=str(exception),
            stacktrace=stacktrace,
        )
        if parent_tile is not None:
            json_obj['parent'] = make_coord_dict(parent_tile)
        if err_details is not None and isinstance(err_details, dict):
            json_obj.update(err_details)
        json_str = json.dumps(json_obj)
        self.logger.error(json_str)

    def error_job_done(self, msg, exception, stacktrace, coord, parent_tile):
        self._log_job_error(
            msg, exception, stacktrace, coord, parent_tile, None)

    def error_job_progress(
            self, msg, exception, stacktrace, coord, parent_tile, err_details):
        self._log_job_error(
            msg, exception, stacktrace, coord, parent_tile, err_details)

    def fetch_error(self, exception, stacktrace, coord, parent):
        json_obj = dict(
            type=log_level_name(LogLevel.ERROR),
            category=log_category_name(LogCategory.PROCESS),
            msg='Fetch error',
            exception=str(exception),
            stacktrace=stacktrace,
        )
        if coord is not None:
            json_obj['coord'] = make_coord_dict(coord)
        if parent is not None:
            json_obj['parent'] = make_coord_dict(parent)
        json_str = json.dumps(json_obj)
        self.logger.error(json_str)


class JsonRawrProcessingLogger(object):

    """Json logger for rawr tile generation and enqueue pipeline"""

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

    def processed(self, n_enqueued, n_inflight, did_rawr_tile_gen, timing,
                  parent_coord):
        json_obj = dict(
            type=log_level_name(LogLevel.INFO),
            category=log_category_name(LogCategory.RAWR_PROCESS),
            did_rawr_tile_gen=did_rawr_tile_gen,
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


class JsonRawrTileLogger(object):

    """Json logger for generating rawr tiles"""

    def __init__(self, logger, run_id):
        self.logger = logger
        self.run_id = run_id

    def error(self, exception, parent, coord):
        stacktrace = format_stacktrace_one_line()
        json_obj = dict(
            type=log_level_name(LogLevel.ERROR),
            category=log_category_name(LogCategory.RAWR_TILE),
            exception=str(exception),
            stacktrace=stacktrace,
            coord=make_coord_dict(coord),
            parent=make_coord_dict(parent),
            run_id=self.run_id,
        )
        json_str = json.dumps(json_obj)
        self.logger.error(json_str)

    def lifecycle(self, parent, msg, *args):
        if args:
            msg = msg % args
        json_obj = dict(
            type=log_level_name(LogLevel.INFO),
            category=log_category_name(LogCategory.LIFECYCLE),
            msg=msg,
            parent=make_coord_dict(parent),
            run_id=self.run_id,
        )
        json_str = json.dumps(json_obj)
        self.logger.info(json_str)

    def coord_done(self, parent, coord, timing):
        json_obj = dict(
            type=log_level_name(LogLevel.INFO),
            msg_type=log_msg_type_name(MsgType.INDIVIDUAL),
            category=log_category_name(LogCategory.RAWR_TILE),
            coord=make_coord_dict(coord),
            parent=make_coord_dict(parent),
            msg='tile processed',
            timing=timing,
            run_id=self.run_id,
        )
        json_str = json.dumps(json_obj)
        self.logger.info(json_str)

    def parent_coord_done(self, parent, timing):
        json_obj = dict(
            type=log_level_name(LogLevel.INFO),
            msg_type=log_msg_type_name(MsgType.PYRAMID),
            category=log_category_name(LogCategory.RAWR_TILE),
            parent=make_coord_dict(parent),
            timing=timing,
            run_id=self.run_id,
        )
        json_str = json.dumps(json_obj)
        self.logger.info(json_str)


class MultipleMessagesTrackerLogger(object):

    def __init__(self, logger):
        self.logger = logger

    def _log(self, msg, coord_id, queue_handle_id):
        z, x, y = coord_id
        json_obj = dict(
            type=log_level_name(LogLevel.WARNING),
            category=log_category_name(LogCategory.PROCESS),
            msg=msg,
            coord=dict(z=z, x=x, y=y),
            handle=queue_handle_id,
        )
        json_str = json.dumps(json_obj)
        self.logger.warning(json_str)

    def unknown_queue_handle_id(self, coord_id, queue_handle_id):
        self._log('Unknown queue_handle_id', coord_id, queue_handle_id)

    def unknown_coord_id(self, coord_id, queue_handle_id):
        self._log('Unknown coord_id', coord_id, queue_handle_id)


class JsonMetaTileLogger(object):
    """
    handle logging for metatile generation command

    parent:  coordinate specified on command line, z7
    pyramid: the top level pyramid coordinate, z10
    coord:   metatile coordinate being generated: z10+
    """
    def __init__(self, logger, run_id):
        self.logger = logger
        self.run_id = run_id

    def _log(self, msg, parent, pyramid=None, coord=None):
        json_obj = dict(
            parent=make_coord_dict(parent),
            type=log_level_name(LogLevel.INFO),
            category=log_category_name(LogCategory.META_TILE),
            msg=msg,
            run_id=self.run_id,
        )
        if pyramid:
            json_obj['pyramid'] = make_coord_dict(pyramid)
        if coord:
            json_obj['coord'] = make_coord_dict(coord)
        json_str = json.dumps(json_obj)
        self.logger.info(json_str)

    def begin_run(self, parent):
        self._log('batch process run begin', parent)

    def end_run(self, parent):
        self._log('batch process run end', parent)

    def begin_pyramid(self, parent, pyramid):
        self._log('pyramid begin', parent, pyramid)

    def end_pyramid(self, parent, pyramid):
        self._log('pyramid end', parent, pyramid)

    def tile_processed(self, parent, pyramid, coord):
        self._log('tile processed', parent, pyramid, coord)

    def _log_exception(self, msg, exception, parent, pyramid, coord=None):
        stacktrace = format_stacktrace_one_line()
        json_obj = dict(
            parent=make_coord_dict(parent),
            pyramid=make_coord_dict(pyramid),
            type=log_level_name(LogLevel.ERROR),
            category=log_category_name(LogCategory.META_TILE),
            msg=msg,
            exception=str(exception),
            stacktrace=stacktrace,
            run_id=self.run_id,
        )
        if coord:
            json_obj['coord'] = make_coord_dict(coord)
        json_str = json.dumps(json_obj)
        self.logger.error(json_str)

    def pyramid_fetch_failed(self, exception, parent, pyramid):
        self._log_exception('pyramid fetch failed', exception, parent, pyramid)

    def tile_fetch_failed(self, exception, parent, pyramid, coord):
        self._log_exception(
            'tile fetch failed', exception, parent, pyramid, coord)

    def tile_process_failed(self, exception, parent, pyramid, coord):
        self._log_exception(
            'tile process failed', exception, parent, pyramid, coord)

    def metatile_storage_failed(self, exception, parent, pyramid, coord):
        self._log_exception(
            'metatile storage failed', exception, parent, pyramid, coord)

    def metatile_already_exists(self, parent, pyramid, coord):
        self._log('metatile already exists', parent, pyramid, coord)


class JsonMetaTileLowZoomLogger(object):
    """
    handle logging for low zoom metatile generation command

    parent: coordinate specified on command line, [z0,z7]
    coord:  metatile coordinate being generated: [z0-z9]
            same as parent unless [z8,z9]
    """
    def __init__(self, logger, run_id):
        self.logger = logger
        self.run_id = run_id

    def _log(self, msg, parent, coord=None):
        json_obj = dict(
            type=log_level_name(LogLevel.INFO),
            category=log_category_name(LogCategory.META_TILE_LOW_ZOOM),
            msg=msg,
            run_id=self.run_id,
            parent=make_coord_dict(parent),
        )
        if coord:
            json_obj['coord'] = make_coord_dict(coord)
        json_str = json.dumps(json_obj)
        self.logger.info(json_str)

    def _log_exception(self, msg, exception, parent, coord):
        stacktrace = format_stacktrace_one_line()
        json_obj = dict(
            type=log_level_name(LogLevel.ERROR),
            category=log_category_name(LogCategory.META_TILE_LOW_ZOOM),
            msg=msg,
            exception=str(exception),
            stacktrace=stacktrace,
            run_id=self.run_id,
            parent=make_coord_dict(parent),
            coord=make_coord_dict(coord),
        )
        json_str = json.dumps(json_obj)
        self.logger.error(json_str)

    def metatile_already_exists(self, parent, coord):
        self._log('metatile already exists', parent, coord)

    def fetch_failed(self, exception, parent, coord):
        self._log_exception('fetch failed', exception, parent, coord)

    def tile_process_failed(self, exception, parent, coord):
        self._log_exception('process failed', exception, parent, coord)

    def metatile_storage_failed(self, exception, parent, coord):
        self._log_exception(
            'metatile storage failed', exception, parent, coord)

    def tile_processed(self, parent, coord):
        self._log('tile processed', parent, coord)

    def begin_run(self, parent):
        self._log('low zoom tile run begin', parent)

    def end_run(self, parent):
        self._log('low zoom tile run end', parent)
