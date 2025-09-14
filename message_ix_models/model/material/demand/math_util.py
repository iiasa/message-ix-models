from typing import Union

import numpy as np
import pandas as pd

GIGA = 10**9
MEGA = 10**6


def steel_function(x: tuple[pd.Series, pd.Series], a: float, b: float, m: float):
    # cons_pcap ~ a * exp(b / gdp_pcap) * (1 - m) ** del_t
    gdp_pcap, del_t = x
    return a * np.exp(b / gdp_pcap) * (1 - m) ** del_t


def cement_function(x: tuple[pd.Series, pd.Series], a: float, b: float):
    gdp_pcap = x[0]
    return a * np.exp(b / gdp_pcap)


def gompertz(phi: float, mu: float, y: Union[pd.Series, float], baseyear: int = 2020):
    return 1 - np.exp(-phi * np.exp(-mu * (y - baseyear)))
