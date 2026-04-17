"""Data structures and data model for Climate Policy Database."""

import builtins
import logging
import re
from dataclasses import dataclass, field, fields
from enum import Enum, auto
from functools import cache
from types import GenericAlias
from typing import TYPE_CHECKING, Any, get_args, get_origin

from message_ix_models.tools.policy import Policy
from message_ix_models.util.pycountry import iso_3166_alpha_3

if TYPE_CHECKING:
    from pycountry.db import Country

log = logging.getLogger(__name__)

#: Aliases for CSV field names. For each entry, the first name appears in recent
#: versions(s) of the database and also corresponds to an attribute of
#: :class:`.NewClimatePolicy`. The second entry appears in certain earlier version(s).
CSV_FIELD_ALIASES = (
    ("supranational_region", "supernational_region"),
    ("subnational_region", "subnational_region_or_state"),
    ("city_or_local", "city"),
    ("country_iso", "country_iso_code"),
    ("country_update", ""),  # No alias, this just ensures the key is present
    ("decision_date", "date_of_decision"),
    ("instrument", "type_of_policy_instrument"),
    ("sector", "sector_name"),
    ("status", "implementation_state"),
    ("start_date", "start_date_of_implementation"),
    ("end_date", "end_date_of_implementation"),
    ("reference", "source_or_references"),
    ("last_update", "last_updated"),
    ("impact_indicators_base_year", "impact_indicator_base_year"),
    ("impact_indicators_comments", "impact_indicator_comments"),
    ("impact_indicators_name", "impact_indicator_name_of_impact_indicator"),
    ("impact_indicators_value", "impact_indicator_value"),
    ("impact_indicators_target_year", "impact_indicator_target_year"),
)


class HIGH_IMPACT(Enum):
    """Enumeration for :attr:`.NewClimatePolicy.high_impact`.

    .. todo:: If a codebook is available identifying what criteria were used to assign
       these codes to the primary source data, reference or quote the defintions for
       each code.

       Do the same for the other enumerations in this module.
    """

    NOTSET = auto()
    High = auto()
    low_medium = auto()
    Unclear = auto()

    #: NB both 'unclear' and 'Unclear' appear in the 2025 draft database as of
    #: 2026-04-17.
    unclear = auto()
    Unknown = auto()


class JURISDICTION(Enum):
    """Enumeration for :attr:`.NewClimatePolicy.jurisdiction`."""

    City = auto()
    Country = auto()
    Subnational_region = auto()


class OBJECTIVE(Enum):
    """Enumeration for :attr:`.NewClimatePolicy.objective`."""

    Adaptation = auto()
    Air_pollution = auto()
    Economic_development = auto()
    Energy_access = auto()
    Energy_security = auto()
    Food_security = auto()
    Mitigation = auto()
    Land_use = auto()
    Water = auto()


class STATUS(Enum):
    """Enumeration for :attr:`.NewClimatePolicy.status`."""

    Draft = auto()
    Ended = auto()
    In_force = auto()
    Planned = auto()
    Superseded = auto()
    Under_review = auto()
    Unknown = auto()


class SECTOR(Enum):
    """Enumeration for :attr:`.NewClimatePolicy.sector`."""

    Agricultural_CH4 = auto()
    Agricultural_CO2 = auto()
    Agricultural_N2O = auto()
    Agriculture_and_forestry = auto()
    Air = auto()
    Appliances = auto()
    Buildings = auto()
    CCS = auto()
    Coal = auto()
    Construction = auto()
    Electricity_and_heat = auto()
    Fluorinated_gases = auto()
    Forestry = auto()
    Fossil_fuel_exploration_and_production = auto()
    Gas = auto()
    General = auto()
    Heating_and_cooling = auto()
    Heavy_duty_vehicles = auto()
    Hot_water_and_cooking = auto()
    Industrial_N2O = auto()
    Industrial_energy_related = auto()
    Industrial_process_CO2 = auto()
    Industry = auto()
    Light_duty_vehicles = auto()
    Low_emissions_mobility = auto()
    Negative_emissions = auto()
    Nuclear = auto()
    Oil = auto()
    Rail = auto()
    Renewables = auto()
    Shipping = auto()
    Transport = auto()
    Unknown = auto()
    Waste_CH4 = auto()


class STRINGENCY(Enum):
    """Enumeration for :attr:`.NewClimatePolicy.stringency`."""

    _0 = 0
    _1 = 1
    _2 = 2
    _3 = 3
    _4 = 4
    NOTSET = auto()


# Ensure lookup of STRINGENCY["1"] gives a valid member
for member in STRINGENCY:
    new_name = member.name[1:]
    if new_name.isdigit():
        # TODO Use the following line once Python 3.13 is the minimum supported version
        # member._add_alias_[new_name]
        # For compatibility with Python 3.10–3.12
        STRINGENCY._member_map_[new_name] = member


class TYPE(Enum):
    """Enumeration for :attr:`.NewClimatePolicy.type`."""

    Energy_service_demand_reduction_and_resource_efficiency = auto()
    Energy_efficiency = auto()
    Non_energy_use = auto()
    Other_low_carbon_technologies_and_fuel_switch = auto()
    Renewables = auto()
    Unknown = auto()


class UPDATE(Enum):
    """Enumeration for :attr:`.NewClimatePolicy.country_update`."""

    Annual = auto()
    Sporadic = auto()

    #: NB This member added only to accommodate database version 2022 and earlier, in
    #: which the respective field does not exist. In versions where the field *does*
    #: exist, its value MUST be one of the two other members.
    NOTSET = auto()


@dataclass(slots=True, unsafe_hash=True)
class NewClimatePolicy(Policy):
    """Policy in the NewClimate data model.

    Properties of this class match the column names appearing in the NewClimate CSV file
    format as of 2024, with the following exceptions:

    - The redundant prefix "policy_" is omitted, for instance "name" instead of
      "policy_name".
    - :attr:`geo` for geography; see the attribute documentation.
    - :attr:`impact_indicators_base_year` and similar have an underscore, rather than
      period (".") in the name.

    For some attributes such as :attr:`country_update`, the type is an enumeration:
    only members of the enumeration may be used. For others, such as :attr:`objective`,
    the type is a :class:`list` of enumeration members, because the database contains
    multiple values, separated by commas. It is unclear if the order in the database is
    meaningful or not, so :class:`list` (rather than :class:`set`) is used to preserve
    the original order.

    .. todo::

       - Add reference(s) to documentation of the data model, if any.
       - Add docstrings for individual fields, quoting the documentation.
       - Parse dates to Python :class:`.datetime` objects.
    """

    #: Country update.
    country_update: UPDATE

    #: MAY be empty.
    decision_date: str

    #: MAY be empty.
    description: str

    #: End date.
    end_date: str

    #: High impact.
    high_impact: HIGH_IMPACT

    #: Unique identifier for the policy.
    id: str

    #: Impact indicator base year.
    impact_indicators_base_year: str

    #: Impact indicator base year.
    impact_indicators_comments: str

    #: Impact indicator name.
    impact_indicators_name: str

    #: Impact indicator target year.
    impact_indicators_target_year: str

    #: Impact indicator value.
    impact_indicators_value: str

    #: Instrument.
    instrument: str

    #: Jurisdiction.
    jurisdiction: JURISDICTION

    #: Last update (of data in the database).
    last_update: str

    #: Name.
    name: str

    #: Objective.
    objective: list[OBJECTIVE]

    #: MAY be empty.
    reference: str

    #: Sector.
    sector: list[SECTOR]

    #: Start date.
    start_date: str

    #: Status.
    status: STATUS

    #: Stringency.
    stringency: STRINGENCY

    #: Title.
    title: str

    #: Type.
    type: list[TYPE]

    #: Geography. MUST be length 1 or greater. Items MAY include:
    #:
    #: 1. English name of supranational region
    #: 2. A :class:`pycountry.db.Country` object.
    #: 3. English name of a subnational region.
    #: 4. English name of a city or locality.
    #:
    #: Some forms visible in the database include:
    #:
    #: - Only (1), for instance :py:`["European Union"]`.
    #: - Only (2).
    #: - Both (2) and (3).
    #: - Both (2) and (4).
    geo: list["str | Country"] = field(default_factory=list)

    def __post_init__(
        self,
    ) -> None:
        # Check that certain fields are non-empty
        for field_name in ("instrument", "name", "title"):
            if getattr(self, field_name) == "":
                raise ValueError(f"{field_name}=''")

        # Replace str with Enum members
        pattern = re.compile("[ /-]")
        for name, enum_type, container in self._enum_fields():
            value = getattr(self, name)
            if isinstance(value, str):
                # Parse the value as a comma-separated list of elements
                values: list[Enum | str] = []
                for v in value.split(","):
                    try:
                        values.append(
                            enum_type[pattern.sub("_", v.strip()) or "NOTSET"]
                        )
                    except KeyError:
                        log.warning(f"Not a member of {enum_type}: {v!r}")
                        values.append(v)

                setattr(
                    self, name, values[0] if container is None else container(values)
                )

    @classmethod
    @cache
    def _enum_fields(
        cls,
    ) -> tuple[tuple[str, builtins.type[Enum], builtins.type | None]]:
        """Return 3-duples of (field name, Enum type, container type)."""
        result = []
        for f in fields(cls):
            if isinstance(f.type, GenericAlias):
                inner = get_args(f.type)[0]
                if isinstance(inner, type) and issubclass(inner, Enum):
                    result.append((f.name, inner, get_origin(f.type)))
            elif isinstance(f.type, type) and issubclass(f.type, Enum):
                result.append((f.name, f.type, None))  # type: ignore

        return tuple(result)  # type: ignore

    @classmethod
    def from_csv_dict(cls, data: dict[str, str]) -> "NewClimatePolicy":
        """Create from a :class:`dict` from a :class:`.csv.CsvReader`.

        This method handles the following transformations:

        - Strip leading white space. Some cells have leading non-printing white space,
          like the UTF-8 byte-order mark \\uFEFF.
        - Replace "." with "_", since the former cannot be used in Python names. For
          example, "impact_indicators.base_year" becomes "impact_indicators_base_year".
        - Remove the redundant prefix "policy_".
        - Handle geographical fields. The CSV format has at least 5 fields that express
          geographical concepts, as well as older aliases:

          1. "city_or_local", "city": Identifier for a city or local geographical unit.
          2. "country": name of a country.
          3. "country_iso", "country_iso_code": ISO 3166 alpha-3 code of a country.
          4. "subnational_region", "subnational_region_or_state": name of a region
             within a country.
          5. "supranational_region", "supernational_region": not used in the 2025
             database. May be in use elsewhere. The name implies it is a name for parts
             or all of 2 or more countries.

          (1), (2), (4) and likely (5) are given in English.

          These are transformed into a single value for the
          :attr:`.NewClimatePolicy.geo` field; see its documentation.
        - Handle older versions of field names appearing in 2022 and earlier database
          versions, per :data:`CSV_FIELD_ALIASES`.
        """
        from pycountry import countries

        # Clean and mogrify field names
        pattern = re.compile("^(\ufeff|policy_)+")
        new_data: dict[str, Any] = {
            pattern.sub("", k).replace(".", "_"): v for k, v in data.items()
        }

        # Handle old names
        for name, name_old in CSV_FIELD_ALIASES:
            values: list[str] = list(
                filter(None, [new_data.pop(name, ""), new_data.pop(name_old, "")])
            )
            match len(values):
                case 0:
                    new_data[name] = ""
                case 1:
                    new_data[name] = values[0]
                case _:
                    msg = f"Both {name}={values[0]} and {name_old}={values[1]}"
                    raise ValueError(msg)

        # Handle geographical fields

        # Candidate entries for the `geo` field, to be filtered
        geo: list[str | Country] = [
            new_data.pop("supranational_region"),
            "",
            new_data.pop("subnational_region"),
            new_data.pop("city_or_local"),
        ]

        # Convert country_iso to a pycountry.db.Country object and check consistency
        country = new_data.pop("country")
        if country_iso := new_data.pop("country_iso"):
            # 2023 database uses an erroneous "URU"
            country_iso = country_iso.replace("URU", "URY")
            if obj := countries.get(alpha_3=country_iso):
                if country and iso_3166_alpha_3(country) != obj.alpha_3:
                    log.warning(f"Name {country!r} does not match ISO 3166-1 {obj}")
                geo[1] = obj
            elif country_iso == "EUE":
                # This is not an ISO 3166-1 code; the EU is a supranational region
                geo[0] = "European Union"
            else:
                raise ValueError(f"country={country!r} country_iso={country_iso!r}")

        # Filter out empty strings
        new_data["geo"] = list(filter(None, geo))

        return cls(**new_data)  # type: ignore

    @property
    def country(self) -> "Country":
        """Return a :mod:`pycountry` object from :attr:`.geo`.

        Raises :class:`ValueError` if none exists.
        """
        for value in self.geo:
            if not isinstance(value, str):
                return value
        raise ValueError
