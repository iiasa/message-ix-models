from collections.abc import Callable, Mapping

import pytest


class MarkFactory:
    """Shorthand to generate test marks."""

    __slots__ = ("_kwarg_formatters", "_mark_name")
    _kwarg_formatters: dict[str, str | Callable]
    _mark_name: str

    def __init__(self, mark_name: str, **kwargs) -> None:
        self._kwarg_formatters = kwargs
        self._mark_name = mark_name

    def __call__(self, args: tuple, kwargs: Mapping) -> pytest.Mark:
        kwargs = {}
        for k, v in self._kwarg_formatters.items():
            match v:
                case str():
                    kwargs[k] = v.format(*args)
                case _:
                    kwargs[k] = v(args)
        return pytest.Mark(name=self._mark_name, args=(), kwargs=kwargs, _ispytest=True)

    def get_inivalue_line(self, name: str) -> str:
        return f"{name}: {self._kwarg_formatters['reason']}"
