"""Transport emissions data."""
import logging
from functools import lru_cache
from typing import Dict

import pandas as pd
from genno import Quantity
from genno.computations import convert_units, load_file, mul
from iam_units import registry
from message_ix import make_df
from message_ix_models import Context
from message_ix_models.util import private_data_path

from message_data.model.transport.utils import path_fallback

log = logging.getLogger(__name__)


def get_emissions_data(context: Context) -> Dict[str, pd.DataFrame]:
    """Load emissions data from a file."""

    fn = f"{context.transport.data_source.emissions}-emission_factor.csv"
    qty = load_file(path_fallback(context, "emi", fn))

    return dict(emission_factor=qty.to_dataframe())


def get_intensity(context: Context) -> pd.DataFrame:
    """Load emissions intensity data from a file."""
    return load_file(private_data_path("transport", "fuel-emi-intensity.csv"))


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


@lru_cache
def _ef_cached(commodity, species, units_in, units_out) -> pd.Series:
    # Product of the input efficiency [energy / activity units] and emissions
    # intensity for the input commodity [mass / energy] → [mass / activity units]
    result = (
        registry.Quantity(1.0, units_in)
        * registry(EI_TEMP.get((species, commodity), "0 g / J"))
    ).to(units_out)
    return result.magnitude, f"{result.units:~}"


def ef_for_input(
    context: Context,
    input_data: pd.DataFrame,
    species: str = "CO2",
    units_out: str = "kt / (Gv km)",
) -> Dict[str, pd.DataFrame]:
    """Calculate emissions factors given data for the ``input`` parameter."""

    # Since pd.DataFrame is not hashable, cannot apply lru_cache() here…
    def _ef(df: pd.DataFrame) -> pd.DataFrame:
        # This call is cached
        ef, unit = _ef_cached(
            df["commodity"].iloc[0], species, df["unit"].iloc[0], units_out
        )
        return df.assign(_ef=ef, unit=unit)

    # Generate emissions_factor data
    # - Create a message_ix-ready data frame; fill `species` as the "emissions" label.
    # - Add the input commodity.
    # - Group by commodity and (input) unit.
    # - Fill the "ef" column with appropriate values and replace "unit".
    # - Compute new value.
    df = (
        make_df("emission_factor", **input_data, emission=species)
        .assign(commodity=input_data["commodity"])
        .groupby(["commodity", "unit"], group_keys=False)
        .apply(_ef)
        .eval("value = value * _ef")
        .drop("_ef", axis=1)
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
