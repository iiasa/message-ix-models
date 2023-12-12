"""Tools for scaling factors.

.. todo:: Add further :class:`.Layer` subclasses beyond :class:`.Constant` for, for
   instance:

   - Linear interpolation between given points.
   - Exponentials, splines, and other functions.
"""
import logging
import operator
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union

import pandas as pd
from genno import Quantity
from genno import operator as g

log = logging.getLogger(__name__)


class Layer(ABC):
    """Base class for layered assumptions used to build up a factor quantification."""

    #: Operation for combining the quantification of this layer with the quantification
    #: of the previous layer. Used by :meth:`apply`.
    #:
    #: Choices might include:
    #:
    #: - :func:`operator.mul`: take the product of this layer and previous layer's
    #:   values.
    #: - :func:`.operator.pow`: raise the previous layer's values to the values from
    #:   this layer. This can be used with :py:`0` and :py:`1` to replace certain
    #:   values in `other` with 1.0, since :py:`other ** 0 = 1.0` and
    #:   :py:`other ** 1 = other`.
    operation: Callable

    @abstractmethod
    def quantify(self, coords: Mapping[str, Any]) -> Quantity:
        """Return a quantification of the layer.

        The returned value:

        - **May** have any of the dimensions indicated by `coords`.
        - For such dimensions, **should** have some or all of the labels in `coords`.

        Subclasses **must** implement this method.
        """
        pass

    def apply(self, other: Quantity, coords: Mapping[str, Any]) -> Quantity:
        """:meth:`.quantify` this layer and combine it with `other`.

        Subclasses **may** override this method. The default implementation combines
        `other` with the results of :meth:`quantify` using :attr:`operation`.
        """
        return self.operation(other, self.quantify(coords))


class Constant(Layer):
    """A value that is constant across 1 or more :attr:`dims`.

    Parameters
    ----------
    value :
       If not :class:`.Quantity`, it is transformed to one.
    dims :
       Either a sequence of :class:`str`, or a single :class:`str` expression like
       "x y z" or "x-y-z", which is split to a sequence.
    """

    #: Fixed value.
    value: Quantity

    #: Dimensions of the result.
    dims: Tuple[str, ...]

    def __init__(self, value: Union[float, Quantity], dims: Union[str, Sequence[str]]):
        self.value = value if isinstance(value, Quantity) else Quantity(value)
        self.dims = (
            tuple(re.split("[ -]", dims)) if isinstance(dims, str) else tuple(dims)
        )

    def quantify(self, coords):
        result = self.value
        # FIXME genno cannot handle multiple dimensions simultaneously
        for d in self.dims:
            result = result.expand_dims(**{d: coords[d]})
        return result


class Omit(Layer):
    """A layer that excludes 1 or more :attr:`labels` along :attr:`dim`.

    Example
    -------
    >>> factor.Omit(x=["x1", "x3"])
    """

    #: Dimension along which to omit :attr:`labels`.
    dim: str

    #: Specific labels or coords to omit.
    labels: list

    operation = operator.pow

    def __init__(self, **kwargs):
        assert 1 == len(kwargs)
        for k, v in kwargs.items():
            self.dim = k
            self.labels = v

    def _mask(self, v_in, v_out, other) -> pd.Series:
        """Return a ‘mask’ for use with :func:`operator.pow`.

        The result has `v_in` where entries of `other` are in :attr:`labels`; otherwise
        `v_out`.
        """
        return pd.Series({x: (v_in if x in self.labels else v_out) for x in other})

    def quantify(self, coords):
        return Quantity(self._mask(0.0, 1.0, coords[self.dim]).rename_axis(self.dim))


class Keep(Omit):
    """A layer that preserves values for 1 or more :attr:`labels` along :attr:`dim`."""

    def quantify(self, coords):
        return Quantity(self._mask(1.0, 0.0, coords[self.dim]).rename_axis(self.dim))


@dataclass
class Map(Layer):
    """A layer that maps to different :attr:`values` along :attr:`dim`.

    Parameters
    ----------
    dim :
       Dimension ID.
    values : optional
       Mapping from labels or coords along `dim` to other :class:`.Layers` which
       produce the value(s) for those coords.
    **value_kwargs :
       Same as `values`, but as keyword arguments.

    Example
    -------
    >>> layer = factor.Map(
    ...     "new_dim",
    ...     x=factor.Constant(2.0, "y z"),
    ...     x=factor.Constant(3.0, "y z"),
    ... )
    """

    dim: str
    values: Dict[str, Layer]

    operation = operator.mul

    def __init__(
        self, dim: str, values: Optional[Dict[str, Layer]] = None, **value_kwargs: Layer
    ):
        self.dim = dim
        self.values = values or value_kwargs

    def quantify(self, coords):
        return g.concat(
            *[
                v.quantify(coords).expand_dims(**{self.dim: k})
                for k, v in self.values.items()
            ]
        )


class ScenarioSetting(Layer):
    """A layer that transforms a ‘scenario’ identifier to a particular ‘setting’.

    This layer handles the common case that multiple ‘scenario’ identifiers may be
    represented in a model using the same quantification. It uses coords along a
    dimension named ‘setting’ to represent these distinct quantifications.

    The :meth:`quantify` and :meth:`apply` methods have special behaviour
    """

    #: Mapping from scenario identifier to setting label.
    setting: Dict[Any, str]

    #: Default setting.
    default: str

    operation = operator.mul

    def __init__(self, setting: Optional[dict] = None, *, default=None, **setting_kw):
        self.setting = setting or setting_kw
        self.default = default

    def __post_init__(self):
        """Check validity of the setting and values."""
        labels0 = set(self.setting.values())
        labels1 = set(self.value.keys())
        if not labels0 <= labels1:
            raise ValueError(
                f"Setting labels {labels0} do not match value labels {labels1}"
            )

    @classmethod
    def of_enum(cls, enum, data, **kwargs):
        """Create from simpler data for an enumeration."""
        setting = {enum[key]: value for key, value in data.items()}

        if set(setting) != set(enum):
            extra = set(enum) - set(setting)
            missing = set(setting) - set(enum)
            raise ValueError(
                f"Scenario identifiers must match members of {enum}; "
                f"missing {missing} and/or extra {extra}"
            )

        return cls(setting=setting, **kwargs)

    def quantify(self, coords):
        """Return a quantification of the layer.

        The key :py:`"scenario"` is **removed** from `coords`. (This means it is not
        available to subsequent layers, and also not included in among the dimensions
        of the :meth:`.Factor.quantify` result.)

        The value returned is the value 1.0 with the single dimension ‘setting’ and
        label obtained by passing the "scenario" coord through :attr:`setting`, or
        using :attr:`default` if it does not appear.
        """
        scenario = coords.pop("scenario")
        try:
            setting = self.setting[scenario]
        except KeyError:
            if not self.default:
                print(self.setting)
                raise
            log.warning(
                f"Use default setting {self.default!r} for unrecognized {scenario}"
            )
            setting = self.default

        return Quantity(1.0).expand_dims(setting=setting)

    def apply(self, other, coords):
        """:meth:`.quantify` this layer and combine it with `other`.

        This drops the ‘setting’ dimension from `other`.
        """
        return super().apply(other, coords).drop_vars("setting")


@dataclass
class Factor:
    """Representation of assumptions used to construct a factor.

    The assumptions are stored as a sequence of :attr:`layers`, and combined one by one
    to produce a multi-dimensional :class:`.Quantity`.

    Factor quantifications can be used in multiplicative, additive, or in other,
    possibly more complicated ways.
    """

    #: Ordered list of :class:`.Layer`.
    layers: List[Layer] = field(default_factory=list)

    def quantify(self, **coords) -> Quantity:
        """Return a quantification.

        The result will have **at least** the dimensions and labels in `coords`, and
        **may** may have additional dimensions not from `coords`.

        Parameters
        ----------
        coords :
            Target :mod:`xarray`-style coords: dimension IDs mapped to lists of labels.
        """
        # Base result: quantify the first layer
        result = self.layers[0].quantify(coords)

        # Handle each layer in sequence
        for layer in self.layers[1:]:
            # Compute the quantification of the layer, apply to the previous result
            result = layer.apply(result, coords)

        # Ensure the result has complete dimensionality and scope
        assert set(result.coords) >= set(coords)
        for k, v in coords.items():
            assert set(v) == set(result.coords[k].data)

        return result
