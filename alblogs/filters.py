
import json
import unittest

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
    def __init__(self, *filters):
        self.filters = filters

    def __call__(self, data: list) -> bool:
        return any(f(data) for f in self.filters)


class Filter:
    def __init__(self, column_number: int, f_comparison: callable, v_comparison, ignore_case: bool = False):
        self.column_number = column_number
        self.f_comparison = f_comparison
        self.v_comparison = v_comparison.casefold() if ignore_case and isinstance(v_comparison, str) else v_comparison
        self.ignore_case = ignore_case

    def __call__(self, data: list) -> bool:
        try:
            return self.f_comparison(self._data(data), self.v_comparison)
        except IndexError:
            raise FilterError("Column number % is out of range" % (self.column_number))

    def _data(self, data: list) -> tuple[str, str]:
        column_data = data[self.column_number]

        if self.ignore_case and isinstance(column_data, str):
            return column_data.casefold() 

        return column_data


class FilterBetween(Filter):
    def __init__(self, column_number: int, minimum: int, maximum: int, inclusive: bool = False):
        if inclusive:
            minimum -= 1
            maximum += 1

        super().__init__(column_number, lambda lhs, rhs: min(rhs) < lhs < max(rhs), (minimum, maximum))


class FilterEquals(Filter):
    def __init__(self, column_number: int, equals, ignore_case: bool = False):
        super().__init__(column_number, lambda lhs, rhs: lhs == rhs, equals, ignore_case)


class FilterNotEquals(Filter):
    def __init__(self, column_number: int, not_equals, ignore_case: bool = False):
        super().__init__(column_number, lambda lhs, rhs: lhs != rhs, not_equals, ignore_case)



class FilterStartsWith(Filter):
    def __init__(self, column_number: int, starts_with: str, ignore_case: bool = False):
        super().__init__(column_number, lambda lhs, rhs: lhs.startswith(rhs), starts_with, ignore_case)


class FilterFactory:
    FILTERS = [
        FilterBetween,
        FilterEquals,
        FilterStartsWith,
    ]

    def __init__(self, logger):
        self.logger = logger

    def __call__(self, configuration_str: str):
        configuration = json.loads(configuration_str) if isinstance(configuration_str, str) else configuration_str
        
        if isinstance(configuration, list):
            return FilterOr(*[self(filter_configuration) for filter_configuration in configuration])
        
        try:
            if 'column_name' in configuration:
                configuration['column_number'] = AlbAccessLogEntry.column_name_to_index(configuration.pop('column_name'))

            Filter = next(Filter for Filter in self.FILTERS if ArgumentIntrospection.constructor_matches(Filter, configuration))

            return Filter(**configuration)
        except StopIteration:
            raise TypeError("%s does not match a filter constructor" % (json.dumps(configuration)))

class FilterFactoryTest(unittest.TestCase):
    def test_filter_single(self):
        factory = FilterFactory("")
        self.assertIsInstance(factory(factory('{"column_name": "user_agent", "equals": "asdf"}'), FilterEquals))
        
    def test_filter_or(self):
        factory = FilterFactory("")
        self.assertIsInstance(factory('[{"column_name": "user_agent", "equals": "xyz"}, {"column_name": "user_agent", "equals": "asdf"}]'), FilterOr)

class FilterTest(unittest.TestCase):
    def test_filter(self):
        filter = Filter(0, lambda lhs, rhs: lhs, 'AsDf', True)
        self.assertEqual('asdf', filter.v_comparison)
        self.assertEqual('asdf', filter(['AsDf']))

        filter = Filter(0, lambda lhs, rhs: lhs, 'AsDf', False)
        self.assertEqual('AsDf', filter.v_comparison)
        self.assertEqual('AsDf', filter(['AsDf']))

        filter = Filter(1, lambda lhs, rhs: lhs, 'AsDf', True)
        self.assertRaises(FilterError, filter, ["asdf"])

    def test_it_should_filter_between(self):
        filter = FilterBetween(0, 100, 200, True)
        self.assertFalse(filter([99]))
        self.assertTrue(filter([100]))
        self.assertTrue(filter([101]))
        self.assertTrue(filter([199]))
        self.assertTrue(filter([200]))
        self.assertFalse(filter([201]))

    def test_it_should_filter_equals(self):
        filter = FilterEquals(0, "ABC")
        self.assertTrue(filter(["ABC"]))
        self.assertFalse(filter(["abc"]))
        self.assertFalse(filter([123]))

        filter = FilterEquals(0, 1)
        self.assertFalse(filter([0]))
        self.assertTrue(filter([1]))