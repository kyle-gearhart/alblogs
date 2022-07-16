import argparse
import logging
import sys

from alblogs.plumber import Plumber
from alblogs.filters import FilterFactory
from alblogs.pipe import PipeFactory
from alblogs.sink import SinkFactory


logger = logging.getLogger('alblogs')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))

filter_factory = FilterFactory(logger)
pipe_factory = PipeFactory(logger)
sink_factory = SinkFactory(logger)

parser = argparse.ArgumentParser(description='Extracts specifc AWS ALB Access log entries')
parser.add_argument('-f', '--filter', type=filter_factory, nargs='+', help='Apply filters to the log entries')
parser.add_argument('-s', '--sink', type=sink_factory, nargs='+', help='Output sinks for matched entries')
parser.add_argument('-d', '--dry', action='store_true', default=False)
parser.add_argument('path', type=pipe_factory)

arguments = parser.parse_args()

logger.debug('='*80 + "\nlaunch options\n" + '='*80)
logger.debug("dry: %s" % (arguments.dry))
logger.debug("paths: %s" % (arguments.path))
logger.debug("filters: %s" % (arguments.filter))
logger.debug("sinks: %s" % (arguments.sink))
logger.debug('='*80)

if not arguments.dry:
    with Plumber(logger, arguments.path, arguments.filter, arguments.sink) as plumber:
        i = plumber()
        logger.info("%s rows processed" % (i))