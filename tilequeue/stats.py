class TileProcessingStatsHandler(object):

    def __init__(self, stats):
        self.stats = stats

    def processed_coord(self, coord_proc_data):
        with self.stats.pipeline() as pipe:
            pipe.timing('process.time.fetch', coord_proc_data.timing['fetch'])
            pipe.timing('process.time.process',
                        coord_proc_data.timing['process'])
            pipe.timing('process.time.upload', coord_proc_data.timing['s3'])
            pipe.timing('process.time.ack', coord_proc_data.timing['ack'])
            pipe.timing('process.time.queue', coord_proc_data.timing['queue'])

            for layer_name, features_size in coord_proc_data.size.items():
                metric_name = 'process.size.%s' % layer_name
                pipe.gauge(metric_name, features_size)

            pipe.incr('process.storage.stored',
                      coord_proc_data.store_info['stored'])
            pipe.incr('process.storage.skipped',
                      coord_proc_data.store_info['not_stored'])

    def processed_pyramid(self, parent_tile,
                          start_time, stop_time):
        duration = stop_time - start_time
        self.stats.timing('process.pyramid', duration)

    def fetch_error(self):
        self.stats.incr('process.errors.fetch', 1)

    def proc_error(self):
        self.stats.incr('process.errors.process', 1)


def emit_time_dict(pipe, timing, prefix):
    for timing_label, value in timing.items():
        metric_name = '%s.%s' % (prefix, timing_label)
        if isinstance(value, dict):
            emit_time_dict(pipe, value, metric_name)
        else:
            pipe.timing(metric_name, value)


class RawrTileEnqueueStatsHandler(object):

    def __init__(self, stats):
        self.stats = stats

    def __call__(self, n_coords, n_payloads, n_msgs_sent,
                 intersect_metrics, timing):

        with self.stats.pipeline() as pipe:
            pipe.gauge('rawr.enqueue.coords', n_coords)
            pipe.gauge('rawr.enqueue.groups', n_payloads)
            pipe.gauge('rawr.enqueue.calls', n_msgs_sent)

            pipe.gauge('rawr.enqueue.intersect.toi',
                       intersect_metrics['n_toi'])
            pipe.gauge('rawr.enqueue.intersect.candidates',
                       intersect_metrics['total'])
            pipe.gauge('rawr.enqueue.intersect.hits',
                       intersect_metrics['hits'])
            pipe.gauge('rawr.enqueue.intersect.misses',
                       intersect_metrics['misses'])
            pipe.gauge('rawr.enqueue.intersect.cached',
                       1 if intersect_metrics['cached'] else 0)

            prefix = 'rawr.enqueue.toi.time'
            emit_time_dict(pipe, timing, prefix)


class RawrTilePipelineStatsHandler(object):

    def __init__(self, stats):
        self.stats = stats

    def __call__(self, n_enqueued, n_inflight, did_rawr_tile_gen, timing):
        with self.stats.pipeline() as pipe:

            pipe.incr('rawr.process.tiles', 1)

            pipe.gauge('rawr.process.enqueued', n_enqueued)
            pipe.gauge('rawr.process.inflight', n_inflight)

            rawr_tile_gen_val = 1 if did_rawr_tile_gen else 0
            pipe.gauge('rawr.process.rawr_tile_gen', rawr_tile_gen_val)

            prefix = 'rawr.process.time'
            emit_time_dict(pipe, timing, prefix)
