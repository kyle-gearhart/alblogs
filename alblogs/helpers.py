
import inspect

class ArgumentIntrospection:
    @classmethod
    def constructor_matches(cls, Class, arguments: dict) -> bool:
        return cls._arguments_and_annotation_match(
            inspect.signature(Class.__init__),
            arguments
        )


    def callable_matches(cls, Callable, arguments: dict) -> bool:
        return cls._arguments_and_annotation_match(
            inspect.signature(Callable),
            arguments
        )


    @classmethod
    def _arguments_and_annotations_match(cls, signature, arguments) -> bool:
        for key, parameter in signature.parameters.items():
            if parameter.default == inspect.Parameter.empty
                if not arguments.has_key(parameter.name)
                    return False

            if parameter.annotation != inspect.Parameter.empty
                if type(arguments[parameter.name]) is not parameter.annotation:
                    return False

        return True
