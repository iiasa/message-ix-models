import numpy as np
from scipy.stats import linregress

from message_ix_models.util import pycountry

from .data_center import get_cisco_region_mapping, get_dc_numbers, get_ignored_countries


def compute_delta_ene(dig0: float, dig1: float, beta: float, eps: float):
    return np.exp(np.log(dig1 / dig0) * beta + eps)


def get_weu_eeu_share() -> float:
    dc_count = get_dc_numbers()

    # drop countries that lack data quality according to Fan et al. 2026
    ignored_isos = [
        pycountry.iso_3166_alpha_3(i)
        for i in get_ignored_countries()
        if isinstance(i, str)
    ]
    dc_count = dc_count[~dc_count["ISO"].isin(ignored_isos)]

    # add column with assigned region of Cisco mapping
    dc_count["Region"] = dc_count["Country"].map(get_cisco_region_mapping())
    dc_count_reg = dc_count.groupby("Region").sum(numeric_only=True)
    result = dc_count_reg.loc["Western Europe"].div(
        dc_count_reg.loc["Western Europe"].add(
            dc_count_reg.loc["Central and Eastern Europe"]
        )
    )
    return float(result.iloc[0])


def run_regression(ln_dig, ln_ene) -> tuple[float, float, float]:
    """Run a linear regression on the given data.

    Parameters
    ----------
    ln_dig :
        Natural logarithm of digitalization data.
    ln_ene :
        Natural logarithm of energy data.
    """
    beta, global_constant, r, p, se = linregress(ln_dig, ln_ene)
    resid = ln_ene.sub(beta * ln_dig + global_constant, axis=0)
    return beta, global_constant, resid


def maybe_return_iso(x: str) -> str:
    """Try to match input string with ISO3 code, otherwise return input.

    Parameters
    ----------
    x :
        Country name to check for ISO 3166-1 alpha-3 code.
    """
    result = pycountry.iso_3166_alpha_3(x)
    if result:
        return result
    return x
