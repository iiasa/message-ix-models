import re
from typing import AnyStr, Generic


class Substitutions(Generic[AnyStr]):
    """Store and apply a sequence of substitutions using :func:`re.sub`.

    Examples
    --------
    Replace "foo" with "bar", then "bar" with "baz", then any 2 or more "z" characters
    with a single "z":

    >>> s = Substitutions(("foo", "bar"), ("bar", "baz"), ("zz+", "z"))
    >>> s("afooz")
    "abaz"
    """

    #: Sequence of replacements. Each 2-tuple corresponds to the first 2 (`pattern`,
    #: `repl`) arguments to :func:`re.sub`.
    pattern_repl: list[tuple[AnyStr | re.Pattern, AnyStr]]

    def __init__(self, *args: tuple[AnyStr | re.Pattern, AnyStr]) -> None:
        self.pattern_repl = list(args)

    def __add__(self, other: tuple[AnyStr | re.Pattern, AnyStr]) -> "Substitutions":
        return type(self)(*self.pattern_repl, other)

    def __call__(self, value: AnyStr) -> AnyStr:
        result = value
        for pattern, repl in self.pattern_repl:
            result = re.sub(pattern, repl, result)
        return result
