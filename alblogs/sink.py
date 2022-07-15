
import csv
import json
import operator

class SinkError(Exception):
    pass



class CsvFileSink:
    def __init__(self, columns: list, target_csv_file: str, deliminator: str = ','):
        self.columns = columns
        self.file = open(target_file, 'w', newline='')
        self.writer = csv.writer(self.file, deliminator)

    def __call__(self, data: list):
        if self.columns:
            self.writer.writerow(list(operator.itemgetter(*self.columns)(data)))

        self.writer.writerow(data)

    def close(self):
        self.writer.close()
        self.file.close()


class SinkFactory:
    SINKS = [
        CsvFileSink
    ]

    def __init__(self, logger):
        self.logger = logger

    def __call__(self, configuration_str: str):
        configuration = json.loads(configuration_str)
        
        try:
            Sink = next(Sink for Sink in self.SINKS if ArgumentIntrospection.constructor_matches(configuration, Sink))

            return Sink(**configuration)
        except StopIteration:
            raise TypeError("%s does not match a sink constructor" % (json.dumps(configuration)))