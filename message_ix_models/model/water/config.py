from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from message_ix_models.util.config import ConfigHelper

if TYPE_CHECKING:
    from message_ix_models import Context


@dataclass
class Config(ConfigHelper):
    """Settings for :mod:`message_ix_models.model.water`."""

    #: Water build variant.
    nexus_set: Literal["cooling", "nexus"] = "nexus"

    #: Climate scenario used for water availability and cooling data.
    RCP: str = "no_climate"

    #: Water SDG policy setting.
    SDG: str = "baseline"

    #: Hydrological-data reliability setting.
    REL: str = "low"

    #: Time slices handled by the water module.
    time: list[str] = field(default_factory=lambda: ["year"])

    #: Whether :attr:`regions` names a global node codelist or a single country.
    type_reg: Literal["country", "global"] = "global"

    #: Single-country model mapping from region code to ISO code.
    map_ISO_c: dict[str, str] = field(default_factory=dict)

    #: Enable basin filtering.
    reduced_basin: bool = False

    #: Basins to add to the automatic reduced-basin selection.
    filter_list: list[str] = field(default_factory=list)

    #: Number of basins per region to keep when :attr:`reduced_basin` is enabled.
    num_basins: int | None = None

    #: Automatic reduced-basin selection method.
    basin_selection: Literal["first_k", "stress"] = "first_k"

    #: Basin names retained after optional filtering.
    valid_basins: set[str] = field(default_factory=set)

    #: Water-module basin node labels, populated during structural mapping.
    all_nodes: Any = None

    @classmethod
    def from_context(cls, context: "Context") -> "Config":
        """Return ``context.water``, creating it if needed."""
        if "water" not in context:
            context["water"] = cls()
        elif isinstance(context["water"], dict):
            context["water"] = cls.from_dict(context["water"])

        return context["water"]
