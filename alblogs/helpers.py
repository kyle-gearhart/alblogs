
import inspect
import unittest

class ArgumentIntrospection:
    @classmethod
    def constructor_matches(cls, Class, arguments: dict) -> bool:
        return cls._arguments_and_annotations_match(
            inspect.signature(Class.__init__),
            arguments
        )


    def callable_matches(cls, Callable, arguments: dict) -> bool:
        return cls._arguments_and_annotations_match(
            inspect.signature(Callable),
            arguments
        )


    @classmethod
    def _arguments_and_annotations_match(cls, signature, arguments) -> bool:
        for i, (name, parameter) in enumerate(signature.parameters.items()):
            if i == 0 and name == 'self':
                continue

            if parameter.default == inspect.Parameter.empty:
                if parameter.name not in arguments:
                    return False
                    
            if parameter.annotation != inspect.Parameter.empty:
                if parameter.name in arguments and type(arguments.get(parameter.name)) is not parameter.annotation:
                    return False

        return not (set(arguments) - set(signature.parameters))


class ArgumentIntrospectionTest(unittest.TestCase):
    class A:
        def __init__(self, a: int, b: str):
            pass

    class B:
        def __init__(self, a: int, b: int):
            pass

    class C:
        def __init__(self, a: int, b: str, c: bool = True):
            pass
    
    def test_it_should_select_class_based_on_args(self):
        arguments_a = { 'a': 1, 'b': 'a'}
        arguments_b = { 'a': 1, 'b': 2 }
        arguments_c = { 'a': 1, 'b': 'a', 'c': False }

        self.assertTrue(ArgumentIntrospection.constructor_matches(self.A, arguments_a))
        self.assertFalse(ArgumentIntrospection.constructor_matches(self.B, arguments_a))
        self.assertTrue(ArgumentIntrospection.constructor_matches(self.C, arguments_a))
        
        self.assertFalse(ArgumentIntrospection.constructor_matches(self.A, arguments_b))
        self.assertTrue(ArgumentIntrospection.constructor_matches(self.B, arguments_b))
        self.assertFalse(ArgumentIntrospection.constructor_matches(self.C, arguments_b))

        self.assertFalse(ArgumentIntrospection.constructor_matches(self.A, arguments_c))
        self.assertFalse(ArgumentIntrospection.constructor_matches(self.B, arguments_c))
        self.assertTrue(ArgumentIntrospection.constructor_matches(self.C, arguments_c))