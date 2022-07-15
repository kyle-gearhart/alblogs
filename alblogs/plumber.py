

from alblogs.alblog import AlbAccessLogEntry

class Plumber:
    def __init__(self, pipe, filters, sinks):
        self.pipe = pipe
        self.filters = filters
        self.sinks = sinks

    def __call__(self):
        for data_row in pipe():
            hydrated_row = AlbAccessLogEntry.hydrate(data_row)

            if all(f(hydrated_row) for f in self.filters):
                [s(hydrated_row) for s in self.sinks]

    def __exit__(self):
        for sink in self.sinks:
            sink.close()