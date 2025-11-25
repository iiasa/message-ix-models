import ixmp
from message_ix import Scenario
from message_ix.report import Reporter

from message_ix_models.report.hydrogen.h2_reporting import (
    ensure_historical_keys,
    load_config,
    pyam_df_from_rep,
    run_h2_reporting,
)

mp = ixmp.Platform("ixmp-dev")
scenario = Scenario(mp, "hyway_SSP_SSP2_v6.4", "SSP2 - Low Emissions_gains")
rep = Reporter.from_scenario(scenario)

df = run_h2_reporting(rep, scenario.model, scenario.scenario)
df = df.timeseries().reset_index()

kws = {
    "t": "meth_bio",
    "nl": "R12_AFR",
    "ya": 2020,
}
hist_act = (
    rep.get("historical_activity")
    .sel(t="meth_bio", nl="R12_AFR", ya=2020)
    .reset_index()
)
hist_out = rep.get("output").sel(t="meth_bio", nl="R12_AFR", ya=2020)
# hist_act * hist_out
gwa_to_ej = 0.03154
meth_bio_to_h2 = 0.937

hist_act["transient h2"] = hist_act["historical_activity"] * gwa_to_ej * meth_bio_to_h2
afr_bio = df[
    (df["region"] == "AFR")
    & (df["variable"] == "Production|Hydrogen|Transient|Methanol|Biomass")
]
value_manual = hist_act["transient h2"].values[0]
value_report = afr_bio[2020].values[0]
print(
    f"value from reporting: {value_manual}, "
    f"value from run_h2_reporting: {value_report}"
)

# --- Reference calculation matching reporting pipeline ---
config = load_config("h2_transient_hist")
ensure_historical_keys(rep)
df_raw = pyam_df_from_rep(rep, config.var, config.mapping).reset_index()

target = (
    df_raw[
        (df_raw["nl"] == "R12_AFR")
        & (df_raw["ya"] == 2020)
        & (df_raw["iamc_name"] == "Transient|Methanol|Biomass w/o CCS")
    ]
    .rename(columns={0: "out_hist"})
    .assign(
        transient_h2=lambda x: x["out_hist"] * gwa_to_ej * x["stoichiometric_factor"]
    )
)

manual_value = target["transient_h2"].sum()
df_report = (
    run_h2_reporting(rep, scenario.model, scenario.scenario).timeseries().reset_index()
)
afr_bio = df_report[
    (df_report["region"] == "AFR")
    & (df_report["variable"] == "Production|Hydrogen|Transient|Methanol|Biomass")
]
report_value = afr_bio[2020].values[0]

print(
    f"value from out_hist pipeline: {manual_value}, "
    f"value from run_h2_reporting: {report_value}"
)
print(
    target[
        [
            "nl",
            "ya",
            "iamc_name",
            "out_hist",
            "stoichiometric_factor",
            "transient_h2",
        ]
    ]
)
mp.close_db()
