import ixmp
import message_ix

mp = ixmp.Platform()

src = message_ix.Scenario(mp, "SSP_SSP2_v6.6", "baseline", cache=False)

print("has_solution:", src.has_solution())                                                                                                      
hist_act = src.par("historical_activity", filters={"technology": "GDP"})
print("historical_activity('GDP') years:", sorted(hist_act["year_act"].unique()) if not hist_act.empty else "EMPTY")
gdp_cal = src.par("gdp_calibrate")
print("gdp_calibrate years:", sorted(gdp_cal["year"].unique()) if not gdp_cal.empty else "EMPTY")

# Also check the timeseries store directly (this is what ghg_reg_dev reads)
ts = src.timeseries()
print("timeseries years present:", sorted(ts["year"].unique()) if not ts.empty else "EMPTY")

mp.close_db()