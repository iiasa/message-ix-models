import numpy as np
import pandas as pd

GIGA = 10**9
MEGA = 10**6


def steel_function(x: tuple[pd.Series, pd.Series], a: float, b: float, m: float):
    r""":math:`cons_pcap = ae^{\frac{b}{gdp_pcap}}(1 - m) ^ {del_t}`"""
    gdp_pcap, del_t = x
    return a * np.exp(b / gdp_pcap) * (1 - m) ** del_t


def cement_function(x: tuple[pd.Series, pd.Series], a: float, b: float):
    r""":math:`cons_pcap = ae^{\frac{b}{gdp_pcap}}`"""
    gdp_pcap = x[0]
    return a * np.exp(b / gdp_pcap)


def gompertz(phi: float, mu: float, y: pd.Series | float, baseyear: int = 2020):
    r""":math:`1 - e^{-\phi e^{-\mu (y - baseyear)}}`"""
    return 1 - np.exp(-phi * np.exp(-mu * (y - baseyear)))


def weibull_cdf(x, lambda_param, beta_param):
    return 1 - np.exp(-((x / lambda_param) ** beta_param))


def weibull_scrap_release(consumption_series, lambda_param=30, beta_param=1.7):
    """
    Vectorized calculation of annual steel scrap release using a Weibull lifetime distribution.

    Parameters:
    - consumption_series: pd.Series with years as index and annual steel consumption (Mt) as values
    - lambda_param: scale parameter (e.g. average lifetime in years)
    - beta_param: shape parameter of the Weibull distribution

    Returns:
    - pd.Series with scrap release per year
    """
    years = consumption_series.index.values

    # Construct matrix of age differences (current_year - past_year)
    age_matrix = years[:, None] - years[None, :]  # shape: (n_years, n_years)

    # Mask out negative ages (future contributions don't make sense)
    age_matrix = np.clip(age_matrix, 0, None)

    # Apply Weibull CDF for age and one year before age (to get incremental release)
    F_t = weibull_cdf(age_matrix, lambda_param, beta_param)
    # F_t_minus1 = weibull_cdf(np.clip(age_matrix - 1, 0, None), lambda_param, beta_param)

    # Calculate release fraction for each (current_year, past_year) pair
    # release_fractions = F_t - F_t_minus1  # shape: (n_years, n_years)
    release_fractions = pd.DataFrame(F_t).sub(pd.DataFrame(F_t).shift(1).fillna(0))

    # Multiply with consumption vector (broadcasted across rows)
    consumption_array = consumption_series.values  # shape: (n_years,)
    scrap_matrix = (
        release_fractions * consumption_array[None, :]
    )  # shape: (n_years, n_years)

    # Sum across columns to get total scrap released in each year
    scrap_release = scrap_matrix.sum(axis=1)

    return pd.Series(scrap_release.values, index=consumption_series.index)
