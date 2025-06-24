"""Add structure and data for emission constraints."""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING

import pandas as pd
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.util import broadcast

if TYPE_CHECKING:
    from message_ix import Scenario
    from pandas import DataFrame, Series


# Shorthand
EF = "emission_factor"
ES = "emission_scaling"
LE = "land_emission"

#: ``type_emission`` supported by :func:`main`.
TYPE_EMISSION = ["TCE_CO2_FFI", "TCE_CO2", "TCE_non-CO2", "TCE_other"]


class METHOD(Enum):
    """Method for :func:`main`."""

    #: Version for e.g. :mod:`project.navigate`.
    A = auto()

    #: Version for |ssp-scenariomip| (:pull:`354`).
    B = auto()


@dataclass
class Data:
    """Data and options for :func:`main`.

    Raises
    ------
    ValueError
        if `type_emission` includes any not in :data:`TYPE_EMISSION`.
    NotImplementedError
        if :py:`use_gains=True` and `method` is :attr:`METHOD.A`.
    """

    # Arguments to main()
    scenario: "Scenario"
    method: METHOD
    type_emission: list[str]
    use_gains: bool

    # Parameter data used in multiple places
    emi_fac: "DataFrame" = field(default_factory=pd.DataFrame)
    lu_co2: "DataFrame" = field(default_factory=pd.DataFrame)

    def __post_init__(self) -> None:
        # Check arguments
        if extra := set(self.type_emission) - set(TYPE_EMISSION):
            raise ValueError(f"Unsupported type_emission = {extra}")
        elif self.method is METHOD.A and self.use_gains:
            raise NotImplementedError("use_gains=True with METHOD.A")

        # Retrieve parameter data used in multiple places
        self.emi_fac = self.scenario.par(EF, filters={"emission": ["TCE"]})
        self.lu_co2 = self.scenario.par(LE, filters={"emission": [self.e_lu]})

    @property
    def e_lu(self) -> str:
        """Emission ID for retrieving land_emission values."""
        return {METHOD.A: "LU_CO2", METHOD.B: "LU_CO2_orig"}[self.method]

    @property
    def t_CO2(self) -> "Series":
        """Mask of rows in :attr:`emi_fac` where "CO2" is in the technology ID."""
        return self.emi_fac.technology.str.contains("CO2")


def main(
    scen: "Scenario",
    *,
    method: METHOD = METHOD.B,
    type_emission: list[str] = ["TCE_CO2", "TCE_non-CO2"],
    use_gains: bool = False,
) -> None:
    """Add structure and data for emission constraints.

    Add all `type_emission` so that constraints for CO₂ and non-CO₂ GHGs can be
    separately defined. According to `type_emission`, parameter data for
    ``emission_factor``, ``emission_scaling``, and/or ``land_emission`` are added by
    calling functions :py:`handle_TCE_*`.

    Parameters
    ----------
    scen : :class:`message_ix.Scenario`
        scenario to which changes should be applied
    method :
        A member of the :class:`METHOD` enumeration.
    type_emission :
        Emission types to be modified. Zero or more of :data:`TYPE_EMISSION`.
    use_gains :
        Affects :func:`handle_TCE_non_CO2` only.

    See also
    --------
    handle_TCE_CO2
    handle_TCE_non_CO2
    handle_TCE_other
    """
    # Check arguments, retrieve some data used in multiple places
    data = Data(scen, method, type_emission, use_gains)

    with scen.transact():
        # Add set elements
        existing = scen.set("type_emission").tolist()
        for type_emi in filter(lambda te: te not in existing, type_emission):
            scen.add_set("type_emission", type_emi)
            scen.add_set("emission", type_emi)
            scen.add_set("cat_emission", [type_emi, type_emi])

        # Copy all emission factors where the technology ID contains "CO2"
        for emission in {"TCE_CO2", "TCE_CO2_FFI"} & set(type_emission):
            # NB The version in message_data assigns TCE_CO2 here even when
            #    emission="TCE_CO2_FFI". Is this intentional? Why?
            scen.add_par(EF, data.emi_fac[data.t_CO2].assign(emission="TCE_CO2"))

        # Call functions to handle other groups of values
        handle_TCE_other(scen, data)
        handle_TCE_CO2(scen, data)
        handle_TCE_non_CO2(scen, data)


def handle_TCE_CO2(scen: "Scenario", data: Data) -> None:
    """Add ``land_emission`` data for emission="TCE_CO2" based on data for :attr:`e_lu`.

    Raises
    ------
    ValueError
        if "TCE_CO2" is in :py:`data.type_emission` **and** :py:`data.lu_co2` is empty.
    """
    te = "TCE_CO2"
    if te not in data.type_emission:
        return
    elif data.lu_co2.empty:
        raise ValueError(f"{LE!r} not available for commodity {data.e_lu!r}")

    scen.add_par(LE, data.lu_co2.assign(emission=te))


def handle_TCE_non_CO2(scen: "Scenario", data: Data) -> None:
    """Add parameter data for emission="TCE_non-CO2".

    Data for ``emission_factor``, ``emission_scaling``, and ``land_emission`` are added.
    """
    te = "TCE_non-CO2"
    if te not in data.type_emission:
        return

    # Copy all emission factors with inverse of string.find(CO2)
    scen.add_par(EF, data.emi_fac[~data.t_CO2].assign(emission=te))

    # Additional modification required for GAINS implementation
    cat_emi = scen.set("cat_emission")
    s_v = (("CH4_TCE", 6.82e-3), ("N2O_TCE", 81.27e-3)) if data.use_gains else ()
    for species, value in s_v:
        # Identify all emission species associated with a given type_emission
        e_species = set(cat_emi.query(f"type_emission=={species!r}").emission)
        if not e_species:
            continue

        # Add cat_emission entries for TCE_non-CO2
        for e in e_species:
            scen.add_set("cat_emission", [te, e])

        # Add emission_scaling values
        df = make_df(ES, type_emission=te, value=value, unit="???").pipe(
            broadcast, emission=e_species
        )
        scen.add_par(ES, df)

    # Combine emission factors (only if use_gains is True)
    # NB Code like this appears in the message_data version, but is commented
    # dims = "node_loc technology year_vtg year_act mode emission unit".split()
    # tce_CH4_TCE = (
    #     scen.par(EF, filters={"emission": CH4_TCE_emi})
    #     .assign(value=lambda df: df.value * 6.82 / 1000, emission=te)
    #     .assign(unit="t C/yr")
    # )
    # tce_N2O_TCE = (
    #     scen.par(EF, filters={"emission": N2O_TCE_emi})
    #     .assign(value=lambda df: df.value * 81.27 / 1000, emission=e)
    #     .assign(unit="t C/yr")
    # )
    # tce_non_co2 = (
    #     tce_N2O_TCE.set_index(dims)
    #     .add(tce_CH4_TCE.set_index(dims), fill_value=0)
    #     .reset_index()
    # )
    # scen.add_par(EF, tce_non_co2)

    # Create emission factor from land_use TCE
    dims = ["node", "land_scenario", "year", "emission", "unit"]

    lu_nonco2 = (
        (
            scen.par(LE, filters={"emission": ["TCE"]})
            .assign(emission=te)
            .set_index(dims)
        )
        - data.lu_co2.assign(emission=te).set_index(dims)
    ).reset_index()

    scen.add_par(LE, lu_nonco2)


def handle_TCE_other(scen: "Scenario", data: Data) -> None:
    """Add parameter data for emission="TCE_other".

    Create emission accounting for land-use CO2 and non-CO2 GHGs and FFI-non-CO2 GHGs
    and shipping related emissions. Non-CO2 GHGs are already included when copying
    emission_factor TCE-non-CO2 hence we only need to copy shipping CO2 emissions.

    Data for ``emission_factor`` and ``land_emission`` are added.
    """
    te = "TCE_other"
    if te not in data.type_emission:
        return

    # Add accounting land-use related emissions
    df = scen.par(LE, filters={"emission": ["TCE"]}).assign(emission=te)
    scen.add_par(LE, df)

    # Add accounting for non-CO2 GHGs from FFI
    df = scen.par(EF, filters={"emission": ["TCE_non-CO2"]}).assign(emission=te)
    scen.add_par(EF, df)

    # Add accounting for shipping related CO2 emissions
    filters = dict(emission=["TCE_CO2"], technology=["CO2s_TCE", "CO2t_TCE"])
    scen.add_par(EF, scen.par(EF, filters=filters).assign(emission=te))


def test_data(
    scenario: "Scenario", emission: list[str] = ["LU_CO2_orig", "TCE"]
) -> None:
    """Add minimal data for testing to `scenario`.

    This includes a bare minimum of data such that :func:`main` runs without error.
    """
    info = ScenarioInfo(scenario)

    land_scenario = ["BIO00GHG000", "BIO06GHG3000"]
    node = ["R12_GLB"]

    land_emission = make_df("land_emission", value=1.0, unit="-").pipe(
        broadcast,
        emission=emission,
        year=info.Y,
        node=info.N + node,
        land_scenario=land_scenario,
    )

    with scenario.transact("Prepare for test of add_alternative_TCE_accounting()"):
        scenario.add_set("emission", emission)
        scenario.add_set("land_scenario", land_scenario)
        scenario.add_set("node", node)
        scenario.add_par("land_emission", land_emission)
