
import json

from alblogs.helpers import ArgumentIntrospection
from alblogs.alblog import AlbAccessLogEntry

class FilterError(Exception):
    pass


class FilterAnd:
    def __init__(self, *filters):
        self.filters = filters

    def __call__(self, data: list) -> bool:
        return all(f(data) for f in self.filters)


class FilterOr:
    def __init__(self, filters):
        self.filters = filters

    def __call__(self, data: list) -> bool:
        return any(f(data) for f in self.filters)


class FilterEquals:
    def __init__(self, column_number: int, equals: str, ignore_case: bool = True):
        self.column_number = column_number
        self.equals = equals.casefold() if ignore_case and isinstance(equals, str) else equals
        self.ignore_case = ignore_case

    def __call__(self, data: list) -> bool:
        try:
            column_data, comparison_data = self._data(data)

            if column_data == comparison_data:
                return True

            return False
        except IndexError:
            raise FilterError("Column number % is out of range" % (self.column_number))

    def _data(self, data: list) -> tuple(str, str):
        column_data = data[self.column_number]

        if self.ignore_case and isinstance(column_data, str):
            return (column_data.casefold(), self.equals, )

        return (column_data, self.equals, )


class FilterFactory:
    FILTERS = [
        FilterEquals,
    ]

    def __call__(self, configuration_str: str):
        configuration = json.loads(configuration_str)
        
        if isinstance(configuration, list):
            return FilterOr(*[self(filter_configuration) for filter_configuration in configuration])
        
        try:
            if configuration.has_key('column_name'):
                configuration['column_number'] = AlbAccessLogEntry.column_name_to_index(configuration.pop('column_name'))

            Filter = next(Filter for Filter in self.FILTERS if ArgumentIntrospection.constructor_matches(configuration, Filter))

            return Filter(**configuration)
        except StopIteration:
            raise TypeError("%s does not match a filter constructor" % (json.dumps(configuration)))
