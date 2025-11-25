import ixmp
from message_ix import Scenario
from message_ix.report import Reporter
from message_ix_models.report.hydrogen.h2_reporting import (
    ensure_historical_keys,
    load_config,
    pyam_df_from_rep,
)

mp = ixmp.Platform("ixmp-dev")

scen = Scenario(mp, "hyway_SSP_SSP2_v6.4", "SSP2 - Low Emissions_gains")
rep = Reporter.from_scenario(scen)
ensure_historical_keys(rep)

config = load_config("h2_transient_hist")

df = pyam_df_from_rep(rep, config.var, config.mapping).reset_index()

sel = df[
    (df["nl"] == "R12_AFR")
    & (df["ya"] == 2020)
    & (df["iamc_name"] == "Transient|Methanol|Biomass w/o CCS")
]

print(sel)
print("raw value", sel[0].values)
