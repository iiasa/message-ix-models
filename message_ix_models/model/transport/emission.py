"""Transport emissions data."""

import logging
from typing import TYPE_CHECKING

import pandas as pd
from genno import Quantity
from genno.operator import convert_units, load_file, mul
from iam_units import registry
from message_ix import make_df

from message_ix_models.util import package_data_path

from .util import region_path_fallback

if TYPE_CHECKING:
    from genno.types import AnyQuantity

    from message_ix_models import Context
    from message_ix_models.types import ParameterData

log = logging.getLogger(__name__)


def get_emissions_data(context: "Context") -> "ParameterData":
    """Load emissions data from a file."""

    fn = f"{context.transport.data_source.emissions}-emission_factor.csv"
    qty = load_file(region_path_fallback(context, "emi", fn))

    return dict(emission_factor=qty.to_dataframe())


def get_intensity(context: "Context") -> "AnyQuantity":
    """Load emissions intensity data from a file."""
    # FIXME use through the build computer
    return load_file(package_data_path("transport", "fuel-emi-intensity.csv"))


def strip_emissions_data(scenario, context):
    """Remove base model's parametrization of freight transport emissions.

    They are re-added by :func:`get_freight_data`.
    """
    log.warning("Not implemented")


# TODO read from configuration
# https://www.eia.gov/environment/emissions/co2_vol_mass.php
# https://www.epa.gov/sites/default/files/2015-07/documents/emission-factors_2014.pdf
EI_TEMP = {
    # This was used temporarily for developing reporting. For a correct value, the
    # emissions intensity of electricity in each region should be reported and
    # multiplied to by the amount of electricity used by transport technologies.
    # ("CO2", "electr"): "10 kg / MBTU",
    ("CO2", "ethanol"): "47.84 kg / MBTU",  # 5.75 kg / gallon
    ("CO2", "gas"): "52.91 kg / MBTU",
    ("CO2", "hydrogen"): "10 kg / MBTU",  # FIXME ditto electr, above
    ("CO2", "lightoil"): "70.66 kg / MBTU",
    ("CO2", "methanol"): "34.11 kg / MBTU",  # 4.10 kg / gallon
}


def ef_for_input(
    context: "Context",
    input_data: pd.DataFrame,
    species: str = "CO2",
    units_out: str = "kt / (Gv km)",
) -> "ParameterData":
    """Calculate emissions factors given data for the ``input`` parameter.

    Parameters
    ----------
    input_data :
        Data for the ``input`` parameter.
    species : str
        Species of emissions.
    units_out : str
        Preferred output units. Should be units of emissions mass (for respective
        species) divided by units of activity (for respective technology).

    Returns
    -------
    pandas.DataFrame
        Data for the ``emission_factor`` parameter.
    """

    def _ef_and_unit(s: pd.Series) -> pd.Series:
        """Look up emission factor multiplier and units given `s`.

        Returns `s` extended with columns "_ef" and "_unit_out".
        """
        c, u = s["commodity"], s["unit"]

        # Product of the input efficiency [energy / activity units] and emissions
        # intensity for the input commodity [mass / energy] → [mass / activity units]
        uq = (
            registry.Quantity(1.0, u) * registry(EI_TEMP.get((species, c), "0 g / J"))
        ).to(units_out)

        return pd.Series(dict(**s, _ef=uq.magnitude, _unit_out=f"{uq.units:~}"))

    # Generate emissions_factor data
    # - Create a message_ix-ready data frame; fill `species` as the "emissions" label.
    # - Add the input commodity.
    # - Merge columns (_ef, _unit_out) computed by _ef_and_unit(). This function runs on
    #   only the unique combinations of (commodity, unit) in `input_data`, or less than
    #   10 rows.
    # - Compute the product of the `input` value and `ef` column.
    # - Restore the expected dimensions.
    df = (
        make_df("emission_factor", **input_data, emission=species)
        .assign(commodity=input_data["commodity"])
        .merge(
            input_data[["commodity", "unit"]]
            .drop_duplicates()
            .apply(_ef_and_unit, axis=1),
            on=["commodity", "unit"],
        )
        .eval("value = value * _ef")
        .drop(["_ef", "commodity", "unit"], axis=1)
        .rename(columns={"_unit_out": "unit"})
    )
    result = dict(emission_factor=df)

    # Emissions intensity values excerpted from existing scenarios
    ei = get_intensity(context).sel(emission=species, drop=True)

    # Name of the relation
    relation = "CO2_trp" if species == "CO2" else f"{species}_Emission"

    if not context.transport.emission_relations:
        pass
    elif not len(ei):
        log.info(f"No emissions intensity values for {relation!r}; skip")
    else:
        # Convert `input` to quantity
        # TODO provide a general function somewhere that does this
        units = input_data["unit"].unique()
        assert 1 == len(units)
        dims = list(filter(lambda c: c not in ("value", "unit"), input_data.columns))
        input_qty = Quantity(input_data.set_index(dims)["value"], units=units[0])

        # Convert units
        # FIXME these units are hard-coded, particular to CO2 in MESSAGEix-GLOBIOM
        ra = convert_units(mul(input_qty, ei), "Mt / (Gv km)")

        # - Convert to pd.DataFrame.
        # - Ensure year_act is integer.
        # - Populate node_rel and year_rel from node_loc and year_act, respectively.
        #   NB eval() approach does not work for strings in node_rel, for some reason.
        # - Drop duplicates.
        tmp = (
            ra.to_series()
            .reset_index()
            .astype({"year_act": int})
            .assign(node_rel=lambda df: df["node_loc"])
            .eval("year_rel = year_act")
            .drop_duplicates(
                subset="node_rel year_rel node_loc technology year_act mode".split()
            )
        )
        name = "relation_activity"
        result[name] = make_df(name, **tmp, relation=relation, unit=f"{ra.units:~}")

    return result
