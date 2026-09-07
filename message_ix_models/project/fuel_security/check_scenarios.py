import ixmp
import message_ix

mp = ixmp.Platform()

base_scen = message_ix.Scenario(mp, model = 'fuel_security', scenario = 'NPi2030')
run_scen = base_scen.clone(model = 'fuel_security', scenario = 'NPi2030_test', keep_solution = False)

run_scen.solve(quiet = False, solve_options={"scaind":"-1"})

mp.close_db()