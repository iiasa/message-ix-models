"""Add alternative emission_types for constraints.

.. caution:: |gh-350|
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from message_ix import Scenario


def main(
    scen: "Scenario",
    *,
    type_emission: list[str] = ["TCE_CO2", "TCE_non-CO2"],
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
        # Create a new type_emission 'TCE_CO2' and 'TCE_non-CO2'
        for type_emi in type_emission:
            if type_emi not in scen.set("type_emission").tolist():
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

        if "TCE_CO2" in type_emission:
            # Create emission factor from land_output 'LU_CO2'
            lu_co2 = scen.par("land_emission", filters={"emission": ["LU_CO2"]})
            if lu_co2.empty:
                raise ValueError("'land_emission' not available for commodity 'LU_CO2'")
            lu_co2.emission = "TCE_CO2"
            scen.add_par("land_emission", lu_co2)

        if "TCE_non-CO2" in type_emission:
            # Copy all emission factors with inverse of string.find(CO2)
            tce_nonco2 = emi_fac[~emi_fac.technology.isin(emi)].assign(
                emission="TCE_non-CO2"
            )

            scen.add_par("emission_factor", tce_nonco2)

            # Create emission factor from land_use TCE - land_ouput 'LU_CO2'
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
