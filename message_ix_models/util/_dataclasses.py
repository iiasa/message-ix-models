"""Utilities for :mod:`.dataclasses`.

This module currently backports one function, :func:`asdict`, from Python 3.13.0, in
order to avoid https://github.com/python/cpython/issues/79721. This issue specifically
occurs with :attr:`.ScenarioInfo.set`, which is of class :class:`defaultdict`. The
backported function **should** be used when (a) Python 3.11 or earlier is in use and (b)
ScenarioInfo is handled directly or indirectly.
"""
# NB Comments are deleted

import copy
import types
from dataclasses import fields

__all__ = [
    "asdict",
]

_ATOMIC_TYPES = frozenset(
    {
        types.NoneType,
        bool,
        int,
        float,
        str,
        complex,
        bytes,
        types.EllipsisType,
        types.NotImplementedType,
        types.CodeType,
        types.BuiltinFunctionType,
        types.FunctionType,
        type,
        range,
        property,
    }
)

_FIELDS = "__dataclass_fields__"


def _is_dataclass_instance(obj):
    return hasattr(type(obj), _FIELDS)


def asdict(obj, *, dict_factory=dict):
    if not _is_dataclass_instance(obj):
        raise TypeError("asdict() should be called on dataclass instances")
    return _asdict_inner(obj, dict_factory)


def _asdict_inner(obj, dict_factory):  # noqa: C901
    obj_type = type(obj)
    if obj_type in _ATOMIC_TYPES:
        return obj
    elif hasattr(obj_type, _FIELDS):
        if dict_factory is dict:
            return {
                f.name: _asdict_inner(getattr(obj, f.name), dict) for f in fields(obj)
            }
        else:
            return dict_factory(
                [
                    (f.name, _asdict_inner(getattr(obj, f.name), dict_factory))
                    for f in fields(obj)
                ]
            )
    elif obj_type is list:
        return [_asdict_inner(v, dict_factory) for v in obj]
    elif obj_type is dict:
        return {
            _asdict_inner(k, dict_factory): _asdict_inner(v, dict_factory)
            for k, v in obj.items()
        }
    elif obj_type is tuple:
        return tuple([_asdict_inner(v, dict_factory) for v in obj])
    elif issubclass(obj_type, tuple):
        if hasattr(obj, "_fields"):
            return obj_type(*[_asdict_inner(v, dict_factory) for v in obj])
        else:
            return obj_type(_asdict_inner(v, dict_factory) for v in obj)
    elif issubclass(obj_type, dict):
        if hasattr(obj_type, "default_factory"):
            result = obj_type(obj.default_factory)
            for k, v in obj.items():
                result[_asdict_inner(k, dict_factory)] = _asdict_inner(v, dict_factory)
            return result
        return obj_type(
            (_asdict_inner(k, dict_factory), _asdict_inner(v, dict_factory))
            for k, v in obj.items()
        )
    elif issubclass(obj_type, list):
        return obj_type(_asdict_inner(v, dict_factory) for v in obj)
    else:
        return copy.deepcopy(obj)
