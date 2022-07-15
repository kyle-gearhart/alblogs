import argparse
import logging

from alblogs.plumber import Plumber
from alblogs.filters import FilterFactory
from alblogs.pipe import PipeFactory
from alblogs.sink import SinkFactory


logger = logging.logger('alblogs')

filter_factory = FilterFactory(logger)
pipe_factory = PipeFactory(logger)
sink_factory = SinkFactory(logger)

parser = argparse.ArgumentParser(description='Extracts specifc AWS ALB Access log entries')
parser.add_argument('-f', '--filter', type=filter_factory)
parser.add_argument('-s', '--sink', type=sink_factory)
parser.add_argument('path', type=pipe_factory)

arguments = parser.parse_args()

with Plumber(arguments.path, arguments.filter, argument.sink) as plumber:
    plumber()