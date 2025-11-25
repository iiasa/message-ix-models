import ixmp
from message_ix.report import Reporter
from message_ix import Scenario

mp = ixmp.Platform("ixmp-dev")
scenario = Scenario(mp, "hyway_SSP_SSP2_v6.4", "SSP2 - Low Emissions_gains")
rep = Reporter.from_scenario(scenario)

## Let's manually add some keys to the reporter:
rep.add("out_hist", "mul", "output", "historical_activity")
rep.add("ref_hist", "mul", "output", "ref_activity")

kws = {
    "t": "meth_bio",
    "nl": "R12_AFR",
    "ya": 2020,
    "yv": 2020,
}

# now let's check what happens to the results of these keys:
df = rep.get("out_hist").sel(kws)
try:
    df_ref = rep.get("ref_hist").sel(kws)
except Exception as e:
    print(f"Error getting ref_hist: {e}")
    df_ref = None

# now we can see that meth_bio doesn't even appear in ref_hist.
# which is wrong... But in the meantime, in out_hist, we get the
# same activity 4 times. Which is also wrong...
out_ya = rep.get("output").sel(kws)
hist_act = rep.get("historical_activity").sel(kws)
out_ya * hist_act

print(df)
print(df_ref)
