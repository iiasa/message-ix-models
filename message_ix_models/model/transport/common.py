from pathlib import Path
import warnings

import pint


# Path for metadata
DATA_PATH = Path(__file__).parents[3] / 'data' / 'transport'


# Model & scenario names
MODEL = {
    'message-transport': dict(
        model='MESSAGEix-Transport',
        scenario='baseline',
        version=1,
        ),
    # For cloning; as suggested by OF
    # TODO find a 'clean-up' version to use
    'base': dict(
        model='CD_Links_SSP2_v2',
        scenario='baseline',
        version='latest',
        ),
}

# Catch a warning from pint 0.10
with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    pint.Quantity([])

# Set up a pint.UnitRegistry
UNITS = pint.UnitRegistry()

# Define all the units in UnitRegistry: i.e. EUR

# Transport units
UNITS.define("""vehicle = [vehicle] = v
passenger = [passenger] = p = pass
tonne_freight = [tonne_freight] = tf = tonnef
vkm = vehicle * kilometer
pkm = passenger * kilometer
tkm = tonne_freight * kilometer
@alias vkm = vkt = v km
@alias pkm = pkt = p km
@alias tkm = tkt = t km""")

# Currencies
# - EUR_2000: Based on Germany's GDP deflator, data from WorldBank
#   https://data.worldbank.org/indicator/
#   NY.GDP.DEFL.ZS?end=2015&locations=DE&start=2000
# - USD_2005: Exchange rate EUR/USD in 2005, data from WorldBank
#   https://www.statista.com/statistics/412794/
#   euro-to-u-s-dollar-annual-average-exchange-rate/

UNITS.define("""EUR_2005 = [currency] = €_2005
EUR_2000 = 0.94662 * EUR_2005 = €_2000
USD_2005 = 1.2435 * EUR_2005 = $_2005""")
