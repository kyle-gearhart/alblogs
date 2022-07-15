
from os import listdir
from os.path import isfile, join

import gzip


class FilePipe:
    def __init__(self, folder_path: str, logger):
        self.folder_path = folder_path
        self.logger = logger

    def __call__(self):
        pass

    def _get_files_in_path(self) -> list:
        return [f for f in listdir(self.folder_path) if isfile(join(self.folder_path, f))]

class GzipPipe(FilePipe):
    def __init__(self, folder_path: str, logger):
        super().__init__(folder_path, logger)

    def __call__(self):
        files = self._get_files_in_path()

        for file_name in files:
            i = 0

            with gzip.open(file_name, 'rt') as f:
                csv_reader = csv.reader(f, delimiter=' ')

                for row in csv_reader:
                    i += 1
                    yield row

            self.logger.info("File %s processed with %s rows" % (file_name, i))
    

class PipeFactory:
    def __init__(self, logger):
        self.logger = logger

    def __call__(self, path: str):
        return GzipPipe(path, self.logger)