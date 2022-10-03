"""Transport emissions data."""
from functools import lru_cache
from typing import Dict

import pandas as pd
from genno.computations import load_file
from iam_units import registry
from message_ix import make_df
from message_ix_models import Context

from message_data.model.transport.utils import path_fallback


def get_emissions_data(context: Context) -> Dict[str, pd.DataFrame]:
    """Load emissions data from a file."""

    fn = f"{context.transport.data_source.emissions}-emission_factor.csv"
    qty = load_file(path_fallback(context, "emi", fn))

    return dict(emission_factor=qty.to_dataframe())


def ef_for_input(
    context: Context,
    input_data: pd.DataFrame,
    species: str = "CO2",
    units_out: str = "kt / (Gv km)",
) -> Dict[str, pd.DataFrame]:
    """Calculate emissions factors given data for the ``input`` parameter."""

    # TODO read from configuration
    # https://www.eia.gov/environment/emissions/co2_vol_mass.php
    # https://www.epa.gov/sites/default/files/2015-07/documents/emission-factors_2014.pdf
    example = {
        ("CO2", "electr"): "10 kg / MBTU",  # FIXME endogenous; identify a usable value
        ("CO2", "ethanol"): "47.84 kg / MBTU",  # 5.75 kg / gallon
        ("CO2", "gas"): "52.91 kg / MBTU",
        ("CO2", "hydrogen"): "10 kg / MBTU",  # FIXME ditto electr, above
        ("CO2", "lightoil"): "70.66 kg / MBTU",
        ("CO2", "methanol"): "34.11 kg / MBTU",  # 4.10 kg / gallon
    }

    # Helper methods for pd.DataFrame.apply(). Since pd.Series is not hashable, cannot
    # apply lru_cache() here
    def _ef0(row: pd.Series) -> pd.Series:
        return _ef1(row["commodity"], row["value"], row["unit"])

    @lru_cache
    def _ef1(commodity, value, unit) -> pd.Series:
        # Product of the input efficiency [energy / activity units] and emissions
        # intensity for the input commodity [mass / energy] → [mass / activity units]
        result = (
            registry.Quantity(value, unit) * registry(example[(species, commodity)])
        ).to(units_out)
        return pd.Series(dict(value=result.magnitude, unit=f"{result.units:~}"))

    # Generate emissions_factor:
    # - Fill `species` as the "emissions" label.
    # - Discard the "value" and "unit" passed through make_df() from `input_data`.
    # - Concatenate column with the retrieved
    data = pd.concat(
        [
            make_df("emission_factor", **input_data, emission=species).drop(
                ["value", "unit"], axis=1
            ),
            input_data.apply(_ef0, axis=1),
        ],
        axis=1,
    )

    return dict(emission_factor=data)
