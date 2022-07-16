
import csv
import json
import operator
import os
import unittest

from alblogs.alblog import AlbAccessLogEntry
from alblogs.helpers import ArgumentIntrospection

class SinkError(Exception):
    pass



class InMemorySink:
    def __init__(self):
        self.data = []
    
    def __call__(self, data):
        self.data.append(data)

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        return a == None


class CsvFileSink:
    @classmethod
    def replace_constructor_args(cls, configuration: dict) -> dict:
        if 'column_names' in configuration:
            configuration = configuration.copy()
            configuration['columns'] = [AlbAccessLogEntry.column_name_to_index(column_name) for column_name in configuration.pop('column_names')]

        return configuration

    def __init__(self, columns: list, target_csv_file: str, delimiter: str = ','):
        self.columns = columns
        self.target_csv_file = target_csv_file
        self.delimiter = delimiter
        self.is_empty = True

    def __call__(self, data: list):
        if self.columns:
            self.writer.writerow(list(operator.itemgetter(*self.columns)(data)))

        self.writer.writerow(data)
        self.is_empty = False

    def destroy_if_empty(self):
        if True == self.is_empty:
            if getattr(self, 'file', None):
                self.file.close()
                self.file = None

                os.unlink(self.target_csv_file)

    def __enter__(self):
        self.file = open(self.target_csv_file, 'w', newline='')
        self.writer = csv.writer(self.file, delimiter=self.delimiter)

        return self

    def __exit__(self, exc_type, exc_value, tb):
        if getattr(self, 'file', None):
            self.file.close()

        return exc_type == None

class TerminalSink:
    def __init__(self, terminal: bool):
        self.terminal = terminal

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        return exc_type == None

    def __call__(self, data: list):
        if self.terminal:
            print(data)


class SinkFactory:
    SINKS = [
        CsvFileSink,
        TerminalSink,
    ]

    def __init__(self, logger):
        self.logger = logger

    def __call__(self, configuration_str: str):
        arguments = json.loads(configuration_str)
        
        try:
            Sink, arguments = next(filter(lambda _: _, (self.constructor_matches(Sink, arguments) for Sink in self.SINKS)))
            return Sink(**arguments)
        except StopIteration:
            raise TypeError("%s does not match a sink constructor" % (json.dumps(arguments)))

    @classmethod
    def constructor_matches(cls, Sink, arguments):
        f_replace_args = getattr(Sink, 'replace_constructor_args', None)

        if callable(f_replace_args):
            arguments = f_replace_args(arguments)

        if ArgumentIntrospection.constructor_matches(Sink, arguments):
            return (Sink, arguments, )

        return None


class SinkFactoryTest(unittest.TestCase):
    def test_it_should_select_csv_file_sink(self):
        factory = SinkFactory("")
        sink = factory("{\"columns\": [], \"target_csv_file\": \"asdf\"}")
        self.assertIsInstance(sink, CsvFileSink)
        sink = factory("{\"column_names\": [\"user_agent\"], \"target_csv_file\": \"asdf\"}")
        self.assertIsInstance(sink, CsvFileSink)

    def test_it_should_not_select_csv_file_sink(self):
        factory = SinkFactory("")
        self.assertRaises(TypeError, factory, "{\"column\": []}")
        
    def test_it_should_select_terminal_sink(self):
        factory = SinkFactory("")
        sink = factory("{\"terminal\": true }")
        self.assertIsInstance(sink, TerminalSink)