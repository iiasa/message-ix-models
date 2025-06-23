"""Add alternative emission_types for constraints.

.. caution:: |gh-350|
"""

from enum import Enum, auto
from typing import TYPE_CHECKING

from message_ix import make_df

if TYPE_CHECKING:
    from message_ix import Scenario


class METHOD(Enum):
    """Method for :func:`add_AFOLU_CO2_accounting`."""

    #: Version for e.g. :mod:`project.navigate`.
    A = auto()

    #: Version subsequent to :pull:`354` and ScenarioMIP7/SSP 2024 update.
    B = auto()


def main(
    scen: "Scenario",
    *,
    method: METHOD = METHOD.B,
    type_emission: list[str] = ["TCE_CO2", "TCE_non-CO2"],
    use_gains: bool = False,
) -> None:
    """Add alternative emission_types for constraints.

    Add alternative emission_types (TCE_CO2 and TCE_non-CO2) so that constraints for
    both CO2 and non-CO2 GHGs can be separately defined. All relevant emission factors
    are added.

    Parameters
    ----------
    scen : :class:`message_ix.Scenario`
        scenario to which changes should be applied
    type_emission :
        Emission types to be modified.
    """

    with scen.transact():
        # Add set elements
        existing = scen.set("type_emission").tolist()
        for type_emi in filter(lambda te: te not in existing, type_emission):
            scen.add_set("type_emission", type_emi)
            scen.add_set("emission", type_emi)
            scen.add_set("cat_emission", [type_emi, type_emi])

        # Copy all emission factors with string.find(CO2)
        if "TCE_CO2" in type_emission or "TCE_CO2_FFI" in type_emission:
            for emission in [
                x for x in type_emission if x in ["TCE_CO2", "TCE_CO2_FFI"]
            ]:
                emi_fac = scen.par("emission_factor", filters={"emission": ["TCE"]})
                emi = [
                    e
                    for e in emi_fac.technology.unique().tolist()
                    if e.find("CO2") >= 0
                ]
                tce_co2 = emi_fac[emi_fac.technology.isin(emi)].assign(
                    emission="TCE_CO2"
                )
                scen.add_par("emission_factor", tce_co2)

        # Create emission accounting for land-use CO2 and non-CO2 GHGs and FFI-non-CO2
        # GHGs and shipping related emissions. Non-CO2 GHGs are already included when
        # copying emission_factor TCE-non-CO2 hence we only need to copy shipping CO2
        # emissions

        # Create new emission type
        if "TCE_other" in type_emission:
            scen.add_set("type_emission", "TCE_other")
            scen.add_set("emission", "TCE_other")
            scen.add_set("cat_emission", ["TCE_other", "TCE_other"])

            # Add accounting land-use related emissions
            df = scen.par("land_emission", filters={"emission": ["TCE"]})
            df.emission = "TCE_other"
            scen.add_par("land_emission", df)

            # Add accounting for non-CO2 GHGs from FFI
            df = scen.par("emission_factor", filters={"emission": ["TCE_non-CO2"]})
            df.emission = "TCE_other"
            scen.add_par("emission_factor", df)

            # Add accounting for shipping related CO2 emissions
            df = scen.par(
                "emission_factor",
                filters={
                    "emission": ["TCE_CO2"],
                    "technology": ["CO2s_TCE", "CO2t_TCE"],
                },
            )
            df.emission = "TCE_other"
            scen.add_par("emission_factor", df)

        if "TCE_CO2" in type_emission:
            e_land_use = {METHOD.A: "LU_CO2", METHOD.B: "LU_CO2_orig"}[method]
            # Create emission factor from land_output 'LU_CO2'
            name = "land_emission"
            lu_co2 = scen.par(name, filters={"emission": [e_land_use]})
            if lu_co2.empty:
                raise ValueError(f"{name!r} not available for commodity {e_land_use!r}")
            lu_co2.emission = "TCE_CO2"
            scen.add_par(name, lu_co2)

        if "TCE_non-CO2" in type_emission:
            # Copy all emission factors with inverse of string.find(CO2)
            tce_nonco2 = emi_fac[~emi_fac.technology.isin(emi)].assign(
                emission="TCE_non-CO2"
            )

            scen.add_par("emission_factor", tce_nonco2)

            # Additional moficiation required for GAINS implementaiton
            if use_gains is True:
                CH4_TCE_emi = (
                    scen.set("cat_emission", filters={"type_emission": "CH4_TCE"})
                    .emission.unique()
                    .tolist()
                )

                for e in CH4_TCE_emi:
                    scen.add_set("cat_emission", ["TCE_non-CO2", e])
                    # Last we add CO2 emissions to the coal powerplant
                    emission_scaling = make_df(
                        "emission_scaling",
                        type_emission="TCE_non-CO2",
                        emission=e,
                        value=0.00682,  # 6.82 / 1000,
                        unit="???",
                    )
                    scen.add_par("emission_scaling", emission_scaling)

                # tce_CH4_TCE = scen.par(
                #    "emission_factor", filters={"emission": CH4_TCE_emi}
                # )
                # tce_CH4_TCE.value *= 6.82 / 1000
                # tce_CH4_TCE.emission = "TCE_non-CO2"

                N2O_TCE_emi = (
                    scen.set("cat_emission", filters={"type_emission": "N2O_TCE"})
                    .emission.unique()
                    .tolist()
                )

                for e in N2O_TCE_emi:
                    scen.add_set("cat_emission", ["TCE_non-CO2", e])
                    # Last we add CO2 emissions to the coal powerplant
                    emission_scaling = make_df(
                        "emission_scaling",
                        type_emission="TCE_non-CO2",
                        emission=e,
                        value=0.08127,  # 81.27 / 1000,
                        unit="???",
                    )
                    scen.add_par("emission_scaling", emission_scaling)

                # tce_N2O_TCE = scen.par(
                #    "emission_factor", filters={"emission": N2O_TCE_emi}
                # )
                # tce_N2O_TCE.value *= 81.27 / 1000
                # tce_N2O_TCE.emission = "TCE_non-CO2"

                # Combine emission factors
                # tce_N2O_TCE = tce_N2O_TCE.assign(unit="t C/yr").set_index(
                #    [
                #        "node_loc",
                #        "technology",
                #        "year_vtg",
                #        "year_act",
                #        "mode",
                #        "emission",
                #        "unit",
                #    ]
                # )
                # tce_CH4_TCE = tce_CH4_TCE.assign(unit="t C/yr").set_index(
                #    [
                #        "node_loc",
                #        "technology",
                #        "year_vtg",
                #        "year_act",
                #        "mode",
                #        "emission",
                #        "unit",
                #    ]
                # )
                # tce_non_co2 = tce_N2O_TCE.add(tce_CH4_TCE, fill_value=0).reset_index()
                # scen.add_par("emission_factor", tce_non_co2)

            # Create emission factor from land_use TCE
            lu_co2.emission = "TCE_non-CO2"
            lu_co2 = lu_co2.set_index(
                ["node", "land_scenario", "year", "emission", "unit"]
            )
            lu_nonco2 = scen.par("land_emission", filters={"emission": ["TCE"]})
            lu_nonco2.emission = "TCE_non-CO2"
            lu_nonco2 = lu_nonco2.set_index(
                ["node", "land_scenario", "year", "emission", "unit"]
            )
            lu_nonco2 = lu_nonco2 - lu_co2
            lu_nonco2 = lu_nonco2.reset_index()
            scen.add_par("land_emission", lu_nonco2)
