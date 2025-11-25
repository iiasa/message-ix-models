#!/usr/bin/env python
import ixmp
from message_ix import Scenario
from message_ix.report import Reporter

mp = ixmp.Platform("ixmp-dev")
scenario = Scenario(mp, "hyway_SSP_SSP2_v6.4", "SSP2 - Low Emissions_gains")

print("Creating reporter...")
rep = Reporter.from_scenario(scenario)
print(f"Initial keys containing 'out_hist': {[k for k in rep.keys() if str(k).startswith('out_hist')]}")

print("Adding out_hist key ...")
rep.add("out_hist", "mul", "output", "historical_activity")
print(f"After add, keys containing 'out_hist': {[k for k in rep.keys() if str(k).startswith('out_hist')]}")

print("Re-instantiating reporter ...")
rep2 = Reporter.from_scenario(scenario)
print(f"New reporter keys containing 'out_hist': {[k for k in rep2.keys() if str(k).startswith('out_hist')]}")
