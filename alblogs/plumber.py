
import concurrent.futures
import itertools
import uuid

from alblogs.filters import FilterError
from alblogs.sink import InMemorySink


class PlumberWorker:
    def __init__(self, logger, pipe, filters):
        self.logger = logger
        self.pipe = pipe
        self.filters = filters
        self.id = uuid.uuid4()

    def __call__(self, sink_creator):
        with sink_creator(str(self.id)) as sink:
            for file, row, data in self.pipe(self.logger):
                try:
                    if self.filters(data):
                        sink(data)
                except FilterError as fe:
                    self.logger.error("Worker %s; File %s; Row %s; FilterError: %s", (self.id, file, row, str(fe)))

        return (sink, self, row)
            

class Plumber:
    def __init__(self, logger, pipe, filters, sinks):
        self.logger = logger
        self.pipe = pipe
        self.filters = filters if isinstance(filters, list) else [filters]
        self.sinks = sinks if isinstance(sinks, list) else [sinks]

    def __call__(self):
        rows_processed = 0
        
        filters = lambda data: all(f(data) for f in self.filters)
        sink_creator = lambda file: InMemorySink()

        with concurrent.futures.ThreadPoolExecutor(max_workers=10, thread_name_prefix='plumber_worker') as executor:
            pipes = map(lambda pipe: pipe, self.pipe(self.logger))
            workers = map(lambda pipe: PlumberWorker(self.logger, pipe, filters), pipes)
            futures = map(lambda worker: executor.submit(worker, sink_creator), workers)

            for future in concurrent.futures.as_completed(futures):
                sink, worker, rows = future.result()

                if worker_error := future.exception():
                    self.logger.error("Worker Status %s: <error> %s" % (worker.id, str(worker_error)))
                    continue
                
                for data, push_to_sink in itertools.product(sink.data, self.sinks):
                    push_to_sink(data)

                rows_processed += rows
                self.logger.info("Worker Status %s: <done> cherry-picked %s of %s rows" % (worker.id, len(sink.data), rows))

        return rows_processed

    def __enter__(self):
        [sink.__enter__() for sink in self.sinks]

        return self

    def __exit__(self, exc_type, exc_value, tb):
        [sink.__exit__(exc_type, exc_value, tb) for sink in self.sinks]