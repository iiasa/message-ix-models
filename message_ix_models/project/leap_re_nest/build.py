# LEAP-RE NEST implementation of a national model

import ixmp as ix
import message_ix
import pandas as pd

from message_ix_models.model.water.reporting import report
from message_ix_models.project.leap_re_nest.reporting_country import report_all_leapre
from message_ix_models.project.leap_re_nest.script import (
    add_grid_shares_OnSSET,
    add_MLED_demand,
    add_WaterCrop,
)
from message_ix_models.project.leap_re_nest.script.add_timeslice import (
    duration_time,
    time_setup,
    xls_to_df,
)
from message_ix_models.project.leap_re_nest.utils import add_cap_bound_fossil, map_basin
from message_ix_models.util import package_data_path

# 1) Generate a Country model. See documentation #

# 2) adjust nodes, years and time-steps
#
# 2.1 add sub-basin nodes

# load a scenario
# IIASA users
# mp = ix.Platform(name="ixmp_dev", jvmargs=["-Xmx14G"])
# external users
mp = ix.Platform(name='local' , jvmargs=['-Xmx14G'])

modelName = "MESSAGEix_ZM"
scenarioName = "single_node"
scen2Name = "sub-units"

# IIASA users
# sc_ref = message_ix.Scenario(mp, modelName, scenarioName, cache=True)
mp.add_unit("km3/month")
mp.add_unit("GWa/month")
# sc_ref.to_excel(package_data_path("projects","leap_re_nest","ref_scen.xlsx") )
# # external users in local database
sc_ref2 = message_ix.Scenario(mp, modelName, "test", version='new',annotation="load from excel")

sc_ref2.read_excel(package_data_path("projects","leap_re_nest","ref_scen.xlsx"),
                    add_units=True,
                    init_items=True,
                    commit_steps=True)
# sc_ref2.commit("")
sc_ref2.solve(solve_options={"lpmethod": "4"},model="MESSAGE")

# for all
# sc = sc_ref.clone(modelName, scen2Name, keep_solution=False)
sc = sc_ref2.clone(modelName, scen2Name,keep_solution=False)

sc.check_out()
# add basins
map_basin(sc)
# check
sc.set("node")
sc.commit("add nodes")

# 2.2 add sub-annual time steps
n_time = 12  # number of time slices <= file ID
file_id = "12"
model_family = "ZMB"
set_update = True  # if True, adds time slices and set adjustments
last_year = 2060  # either int (year) or None (removes extra years)
node_exlude = ["World"]

xls_file = "input_data_" + file_id + "_" + model_family + ".xlsx"
path_xls = package_data_path("projects", "leap_re_nest", xls_file)

if sc.has_solution():
    sc.remove_solution()

nodes = [x for x in sc.set("node") if x not in ["World"] + node_exlude]

# 2.2.1) Loading Excel data (time series)
xls = pd.ExcelFile(path_xls)

# 2.2.1) Updating sets related to time
# Adding subannual time slices to the relevant sets
duration, df_time, dict_xls = xls_to_df(xls, n_time, nodes)
times = df_time["time"].tolist()

if set_update:
    time_setup(sc, df_time, last_year)
    duration_time(sc, df_time)
    if last_year:
        df = sc.par("bound_activity_up")
        assert max(set(df["year_act"])) <= last_year

sc.set("map_time")
sc.set_as_default()

# scen_list = mp.scenario_list(default=False)
# scen_list = scen_list[(scen_list['model']==modelName)]

# 3) Demand processing

# run different project scenarios: baseline, moderate_development, sustainable_development
scens = ["baseline", "improved", "ambitious"]
for ss in scens:
    scen3Name = "MLED_" + ss
    sc3 = sc.clone(modelName, scen3Name, keep_solution=False)
    print("Scenario: ", sc3.scenario)
    add_MLED_demand.main(sc3, ss)  # to be adapted for scenarios
    add_grid_shares_OnSSET.main(sc3, ss)

    caseName = sc3.model + "__" + sc3.scenario + "__v" + str(sc3.version)
    # Solving the model
    sc3.solve(solve_options={"lpmethod": "4"}, model="MESSAGE", case=caseName)
    sc3.set_as_default()

# %%4) add water structure

# when using the CLI it would be something like
# with the correct scenario name
# mix-models --url=ixmp://ixmp_dev/MESSAGEix_ZM/MLED_baseline water-ix --regions=ZMB nexus --rcps=7p0 --rels=low
# mix-models --url=ixmp://ixmp_dev/MESSAGEix_ZM/MLED_improved water-ix --regions=ZMB nexus --rcps=7p0 --rels=low --sdgs=improved
# mix-models --url=ixmp://ixmp_dev/MESSAGEix_ZM/MLED_ambitious water-ix --regions=ZMB nexus --rcps=7p0 --rels=low --sdgs=ambitious


# %% 5) add irrigation and adjust electricity uses in the water
mp.add_timeslice(name="year", category="Common", duration=1)
for mm in range(1, 13):
    # print(str(mm))
    mp.add_timeslice(name=str(mm), category="month", duration=0.08333)

scens = ["baseline", "improved", "ambitious"]
for ss in scens:
    scen_nex_name = "MLED_" + ss + "_nexus"
    scen4Name = "MLED_" + ss + "_nexus_full"
    sc_nexus = message_ix.Scenario(mp, modelName, scen_nex_name, cache=True)
    sc4 = sc_nexus.clone(modelName, scen4Name, keep_solution=False)
    add_WaterCrop.main(sc4, ss)
    if ss == "ambitious":
        # blocking fossil new capacity after 2020
        add_cap_bound_fossil(sc4)
    caseName = sc4.model + "__" + sc4.scenario + "__v" + str(sc4.version)
    # Solving the model
    sc4.solve(solve_options={"lpmethod": "4"}, model="MESSAGE", case=caseName)
    sc4.set_as_default()
    # report
    reg = model_family
    sdgs = False
    report_all_leapre(sc4, reg, sdgs)


# mp.timeslices()
# sc = message_ix.Scenario(mp, modelName, scen4Name, cache=True)
# # ADD GDP and Pop info in the timeseries
# from message_ix_modelsodels.project.leap_re_nest.script.plotter import plotter

# out_path = str(package_data_path().parents[0] / "reporting_output/")
