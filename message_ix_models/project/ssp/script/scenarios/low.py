"""Script for SSP/ScenarioMIP ‘low’ scenario.

Originally added via :pull:`235`, cherry-picked and merged in :pull:`340`.

.. todo::
   - Collect code in a function.
   - Expose function through the CLI.
   - Add tests.
   - Add documentation.
"""

import ixmp  # type: ignore
import message_ix  # type: ignore

from message_ix_models.project.ssp.script.util.functions import (
    add_balance_equality,
    add_steel_sector_nze,
    modify_steel_growth,
    modify_steel_initial,
    remove_bof_steel_lower,
)

# selections
sel_scen = "SSP1"
scen_suffix = ""
rem_bof_steel = True
mod_growth_steel = True
mod_initial_steel = True
add_steel_target = False

# parameters
trp_year_start = 2035
mult_price = 5.5
rc_years = [2060, 2070, 2080, 2090, 2100, 2110]
steel_years = [2030, 2035, 2040, 2045, 2050, 2055, 2060, 2070, 2080, 2090, 2100, 2110]
steel_growth = 0.075
steel_inital = 1.0
nze_targets = [
    4.2,
    3.7,
    3.2,
    2.7,
]

# model and scenario names
snames = {"SSP1": "SSP1 - Low Emissions", "SSP2": "SSP2 - Low Emissions"}
svers = {"SSP1": 1, "SSP2": 1}
model_orig = "SSP_" + sel_scen + "_v1.0"
scenario_orig = snames[sel_scen]

if rem_bof_steel:
    scen_suffix += "_bof"
if mod_growth_steel:
    scen_suffix += "_growth"
if mod_initial_steel:
    scen_suffix += "_initial"
if add_steel_target:
    scen_suffix += "_nzsteel"

# target scenario
model_target = "MM_ScenarioMIP"
scenario_target = "Low_" + sel_scen + scen_suffix  # + "_v" + str(scen_vers)

# connect to database
mp = ixmp.Platform("ixmp_dev")

# load scenario
s_orig = message_ix.Scenario(
    mp, model=model_orig, scenario=scenario_orig, version=svers[sel_scen]
)

# clone scenario
s_tar = s_orig.clone(model_target, scenario_target, keep_solution=False)
s_tar.set_as_default()

# modify steel sector
if rem_bof_steel:
    remove_bof_steel_lower(s_tar, steel_years)

if mod_growth_steel:
    modify_steel_growth(
        s_tar,
        ["dri_gas_steel", "dri_h2_steel", "eaf_steel"],
        steel_years,
        steel_growth,
    )

if mod_initial_steel:
    modify_steel_initial(
        s_tar,
        ["dri_gas_steel", "dri_h2_steel"],
        steel_years,
        steel_inital,
    )

if add_steel_target:
    add_steel_sector_nze(s_tar, nze_targets)

# add balance equality
add_balance_equality(s_tar)

solve_typ = "MESSAGE-MACRO"
solve_args = dict(model=solve_typ)
s_tar.solve(**solve_args)
s_tar.set_as_default()

mp.close_db()
