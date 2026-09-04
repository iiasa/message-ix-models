import ixmp
import message_ix

mp = ixmp.Platform()

src = message_ix.Scenario(mp, "SSP_SSP2_v5.1", "baseline", cache=False)                                                                         
print("SOURCE has_solution:", src.has_solution())
gdp_var = src.var("GDP")
print("SOURCE var('GDP') empty:", gdp_var.empty)
if not gdp_var.empty:
    print("SOURCE var('GDP') years:", sorted(gdp_var["year"].unique()))                                                                         
gdp_par = src.par("bound_activity_lo", filters={"technology": "GDP"})
print("SOURCE bound_activity_lo('GDP') years:", sorted(gdp_par["year_act"].unique()) if not gdp_par.empty else "EMPTY")

# Check the cloned baseline_DEFAULT
base = message_ix.Scenario(mp, "fuel_security", "baseline_DEFAULT", cache=False)
print("CLONE has_solution:", base.has_solution())                                                                                               
gdp_var2 = base.var("GDP")
print("CLONE var('GDP') empty:", gdp_var2.empty)
if not gdp_var2.empty:
    print("CLONE var('GDP') years:", sorted(gdp_var2["year"].unique()))
gdp_par2 = base.par("bound_activity_lo", filters={"technology": "GDP"})
print("CLONE bound_activity_lo('GDP') years:", sorted(gdp_par2["year_act"].unique()) if not gdp_par2.empty else "EMPTY")

mp.close_db()