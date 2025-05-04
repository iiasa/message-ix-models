"""In-line checks for :mod:`genno` graphs."""

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Collection, Hashable, Mapping
from dataclasses import dataclass
from functools import partial
from itertools import count
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Optional, TypeVar, Union

import genno
import pandas as pd

if TYPE_CHECKING:
    import pint.registry

    from message_ix_models.types import KeyLike

    T = TypeVar("T")

log = logging.getLogger(__name__)


@dataclass
class Check(ABC):
    """Class representing a single check."""

    types: ClassVar[tuple[type, ...]] = ()

    def __post_init__(self) -> None:
        if not self.types:
            log.error(f"{type(self).__name__}.types is empty → check will never run")

    def __call__(self, obj) -> tuple[bool, str]:
        if not isinstance(obj, self.types):
            # return True, f"{type(obj)} not handled by {type(self)}; skip"  # DEBUG
            return True, ""
        else:
            result = self.run(obj)
            if isinstance(result, tuple):
                return result
            else:
                return result, ""

    @property
    def _description(self) -> str:
        """Description derived from the class docstring."""
        assert self.__doc__
        return self.__doc__.splitlines()[0].rstrip(".")

    def recurse_parameter_data(self, obj) -> tuple[bool, str]:
        """:func:`run` recursively on :any:`.ParameterData`."""
        _pass, fail = [], {}

        for k, v in obj.items():
            ret = self.run(v)
            result, message = ret if isinstance(ret, tuple) else (ret, "")
            if result:
                _pass.append(k)
            else:
                fail[k] = message

        lines = [f"{self._description} in {len(_pass)}/{len(obj)} parameters"]
        if fail:
            lines.extend(["", f"FAIL: {len(fail)} parameters"])
            lines.extend(f"{k!r}:\n{v}" for k, v in fail.items())

        return not bool(fail), "\n".join(lines)

    @abstractmethod
    def run(self, obj) -> Union[bool, tuple[bool, str]]:
        """Run the check on `obj` and return either :any:`True` or :any:`False`."""


class CheckResult:
    """Accumulator for the results of multiple checks.

    This class' :meth:`__call__` method can be used as the :py:`result_cb` argument to
    :func:`.apply_checks`. After doing so, :py:`bool(check_result)` will give the
    overall passage or failure of the check suite.
    """

    def __init__(self):
        self._pass = True

    def __bool__(self) -> bool:
        return self._pass

    def __call__(self, value: bool, message: str) -> None:
        self._pass &= value
        if message:
            log.log(logging.INFO if value else logging.ERROR, message)


@dataclass
class ContainsDataForParameters(Check):
    #: Collection of parameter names that should be present in the object.
    parameter_names: set[str]

    types = (dict,)

    def run(self, obj):
        if self.parameter_names:
            if self.parameter_names != set(obj):
                both = self.parameter_names & set(obj)
                msg = (
                    f"{len(both)}/{len(self.parameter_names)} expected parameters are"
                    " present"
                )
                if missing := self.parameter_names - set(obj):
                    msg += f"\nExpected but missing parameters: {missing}"
                if extra := set(obj) - self.parameter_names:
                    msg += f"\nUnexpected, extra parameters: {extra}"

                return (False, msg)
            else:
                N = len(self.parameter_names)
                return True, f"{N}/{N} expected parameters are present"
        return True


@dataclass
class Dump(Check):
    """Dump to a temporary path for inspection.

    This always returns :any:`True`.
    """

    base_path: Path

    types = (dict, pd.DataFrame, genno.Quantity)

    def recurse_parameter_data(self, obj) -> tuple[bool, str]:
        for k, v in obj.items():
            self.run(v, name=k)

        return True, ""

    def run(self, obj, *, name: Optional[str] = None):
        if isinstance(obj, dict):
            return self.recurse_parameter_data(obj)

        # Construct a path that does not yet exist
        name_stem = name or "debug"
        name_seq = map(lambda i: f"{name_stem}-{i}", count())
        while True:
            path = self.base_path.joinpath(next(name_seq)).with_suffix(".csv")
            if not path.exists():
                break

        # Get a pandas object
        pd_obj = (
            obj.to_series().reset_index() if isinstance(obj, genno.Quantity) else obj
        )

        log.info(f"Dump data to {path}")
        pd_obj.to_csv(path, index=False)

        return True, ""


@dataclass
class HasCoords(Check):
    """Object has/omits certain coordinates."""

    coords: dict[str, Collection[Hashable]]
    inverse: bool = False
    types = (dict, pd.DataFrame, genno.Quantity)

    def run(self, obj):
        if isinstance(obj, dict):
            return self.recurse_parameter_data(obj)

        # Prepare a coords mapping for the object
        if isinstance(obj, pd.DataFrame):
            # Unique values in each column
            coords = {dim: set(obj[dim].unique()) for dim in obj.columns}
        else:
            # genno or xarray coords → mapping of str → xr.DataArray; unpack the latter
            coords = {k: set(v.data) for k, v in obj.coords.items()}

        result = True
        message = []
        for dim, v in self.coords.items():
            if dim not in coords:
                continue
            d, exp, obs = f"Dimension {dim!r}", set(v), coords[dim]

            if not self.inverse and not exp <= obs:
                result = False
                message.append(f"{d} is missing coords {exp - obs}")
            elif self.inverse and not exp.isdisjoint(obs):
                result = False
                message.append(f"{d} has unexpected coords {exp ^ obs}")
            else:
                message.append(f"{d} has {len(exp)}/{len(exp)} expected coords")
        return result, "\n".join(message)


@dataclass
class HasUnits(Check):
    """Quantity has the expected units."""

    units: Optional[Union[str, dict, "pint.registry.Quantity", "pint.registry.Unit"]]
    types = (genno.Quantity, pd.DataFrame, dict)

    def run(self, obj):
        from genno.testing import assert_units as a_u_genno

        from message_ix_models.model.transport.testing import assert_units as a_u_local

        if isinstance(obj, dict):
            return self.recurse_parameter_data(obj)

        if self.units is None:
            return True

        if isinstance(self.units, dict) or isinstance(obj, pd.DataFrame):
            func = a_u_local
            if isinstance(obj, genno.Quantity):
                obj = obj.to_series().reset_index()
        else:
            func = a_u_genno

        try:
            func(obj, self.units)
        except AssertionError as e:
            return False, f"Expected {e!s}"
        else:
            return True, f"Units are {self.units!r}"


@dataclass
class NoneMissing(Check):
    """No missing values."""

    setting: None = None
    types = (pd.DataFrame, dict)

    def run(self, obj):
        if isinstance(obj, dict):
            return self.recurse_parameter_data(obj)

        missing = obj.isna()
        if missing.any(axis=None):
            return False, "NaN values in data frame"
        return True, self._description


@dataclass
class NonNegative(Check):
    """No negative values.

    .. todo:: Add args so the check can be above or below any threshold value.
    """

    types = (pd.DataFrame, dict)

    def run(self, obj):
        if isinstance(obj, dict):
            return self.recurse_parameter_data(obj)

        result = obj["value"] < 0
        if result.any(axis=None):
            return False, f"Negative values for {result.sum()} observations"
        return True, self._description


@dataclass
class Log(Check):
    """Log contents.

    This always returns :any:`True`.
    """

    #: Number of rows to log.
    rows: Optional[int] = 7

    types = (dict, pd.DataFrame, genno.Quantity)

    def recurse_parameter_data(self, obj) -> tuple[bool, str]:
        for k, v in obj.items():
            sep = f"{k!r} -----"
            log.debug(sep)
            self.run(v)

        return True, ""

    def run(self, obj):
        if isinstance(obj, dict):
            return self.recurse_parameter_data(obj)

        # Get a pandas object that has a .to_string() method
        pd_obj = obj.to_series() if isinstance(obj, genno.Quantity) else obj

        lines = [
            f"{len(pd_obj)} observations",
            pd_obj.to_string(max_rows=self.rows, min_rows=self.rows),
        ]
        log.debug("\n".join(lines))

        return True, ""


@dataclass
class Size(Check):
    """Quantity has expected size on certain dimensions."""

    setting: dict[str, int]
    types = (genno.Quantity,)

    def run(self, obj):
        result = True
        message = []
        for dim, N in self.setting.items():
            if dim not in obj.dims:
                continue
            if N != len(obj.coords[dim]):
                message.append(
                    f"Dimension {dim!r} has length {len(obj.coords[dim])} != {N}"
                )
                result = False
            else:
                message.append(f"Dimension {dim!r} has length {N}")
        return result, "\n".join(message)


def apply_checks(
    value: "T",
    checks: Collection[Check],
    *,
    key: "KeyLike",
    result_cb: Callable[[bool, str], None],
) -> "T":
    """Apply some `checks` to `value`.

    Parameters
    ----------
    value
        Anything.
    checks
        0 or more :class:`.Check` instances. Each is called on `value`.
    key
        Used to log information about the checks performed.
    result_cb
        Callback function that is passed the result of each :class:`.Check` call.

    Returns
    -------
    Any
        `value` exactly as passed.
    """
    separator = f"=== {key!r}: {len(checks)} checks ==="
    log.info(separator)

    # Invoke each of the checks, accumulating the result via `result_cb`
    for check in checks:
        result_cb(*check(value))

    log.info("=" * len(separator))

    # Pass through the original value
    return value


def insert_checks(
    computer: "genno.Computer",
    target: "KeyLike",
    check_map: Mapping["KeyLike", Collection["Check"]],
    check_common: Collection["Check"],
) -> CheckResult:
    """Insert some :class:`Checks <.Check>` into `computer`.

    Parameters
    ----------
    computer
    target
        A new key added to trigger all the tasks and checks.
    check_map
        A mapping from existing keys (that must appear in `computer`) to collections of
        :class:`.Check` instances to be applied to those keys.
    check_common
        A collection of common :class:`.Check` instances, to be applied to every key in
        `check_map`.

    Returns
    -------
    CheckResult
        after the checks are triggered (for instance, with :py:`computer.get(target)`),
        this object will contain the overall check pass/fail result.
    """
    # Create a CheckResult instance to absorb the outputs of each apply_checks() call
    # and sub-call
    result = CheckResult()

    # Iterate over keys mentioned in `check_map`
    for key, checks in check_map.items():
        # A collection of Check instances, including those specific to `key` and those
        # from `check_common`
        c = tuple(checks) + tuple(check_common)
        # Insert a task with apply_checks() as the callable; move the existing task to
        # "{key}+pre"
        computer.insert(
            key, partial(apply_checks, key=key, checks=c, result_cb=result), ...
        )

    # Add a task at `target` that collects the outputs of every inserted call
    computer.add(target, list(check_map))

    return result


def verbose_check(verbosity: int, tmp_path: Optional[Path] = None) -> list[Check]:
    """Return 0 or more checks that display the data to which they are applied.

    These may be appended to collections passed as inputs to :func:`.insert_checks`.

    Parameters
    ----------
    verbosity : int
        0. Don't log anything
        1. Log :attr:`.Log.rows` values at the start/end of each quantity.
        2. Log *all* data. This can produce large logs, e.g. more than 1 GiB of text for
           :func:`.tests.model.transport.test_build.test_debug`.
        3. Dump all data to files in `tmp_path`.
    """
    from message_ix_models.testing.check import Dump, Log

    values: dict[int, list[Check]] = {
        0: [],
        1: [Log()],
        2: [Log(None)],
        3: [Dump(tmp_path or Path.cwd())],
    }

    return values[verbosity]
