
from os import listdir
from os.path import isfile, join

import csv
import glob
import gzip
import json
import pathlib
import unittest


class AwsS3Pipe:
    def __init__(self, bucket: str, folder_path: str = '/'):
        pass


class GzipFile:
    def __init__(self, file_path: str):
        self.file_path = file_path

    @classmethod
    def supports(cls, file_path: str):
        path = pathlib.Path(file_path)
        return '.gz' == path.suffix

    def __call__(self, logger):
        try:
            with gzip.open(self.file_path, 'rt') as f:
                csv_reader = csv.reader(f, delimiter=' ')

                for row, data in enumerate(csv_reader):
                    yield self.file_path, row, data

                f.close()

        except gzip.BadGzipFile:
            logger.error("File %s is not a valid gzip file" % (self.file_path))


class FilePipe:
    FILE_TYPES = [
        GzipFile,
    ]

    def __init__(self, folder_path: str):
        self.folder_path = folder_path

    def __call__(self, logger):
        files = self._get_files_in_path()

        for Handler, file_path in self._get_file_handlers(files):
            if not Handler:
                logger.error('File %s has no handler' % (file_path))
                continue

            handler = Handler(file_path)

            yield handler

    def _get_files_in_path(self) -> set:
        if '*' in self.folder_path:
            return set(glob.glob(self.folder_path))

        return set('/'.join([self.folder_path, f]) for f in listdir(self.folder_path) if isfile(join(self.folder_path, f)))

    def _get_file_handlers(self, files: set):
        return ((next((Handler for Handler in self.FILE_TYPES if Handler.supports(file_path)), None), file_path) for file_path in files)

class PipeFactory:
    PIPES = [
        AwsS3Pipe,
        FilePipe,
    ]

    def __init__(self, logger):
        self.logger = logger

    def __call__(self, configuration_str: str):
        try:
            configuration = json.loads(configuration_str)
        except json.decoder.JSONDecodeError:
            configuration = configuration_str

        if isinstance(configuration, str):
            return FilePipe(configuration)


class PipeFactoryTest(unittest.TestCase):
    def test_it_should_load_file_pipe(self):
        factory = PipeFactory("logger")
        pipe = factory("asdf")
        self.assertIsInstance(pipe, FilePipe)

        pipe = factory('{"file_path": "asdf"}')
        self.assertIsInstance(pipe, FilePipe)