import ixmp
import message_ix

project_name = 'fuel_security'
base_scenario_name = 'baseline_bilateral'

mp = ixmp.Platform()

print("Update to myopic run")
out_scenario = message_ix.Scenario(mp, model=project_name, scenario=base_scenario_name)
if out_scenario.has_solution():
    out_scenario.remove_solution()
out_scenario.solve(quiet = False,
                   model = 'MESSAGE',
                   solve_options={"scaind":"-1"},
                   gams_args=['--foresight=10'])
mp.close_db()