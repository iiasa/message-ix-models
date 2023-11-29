import ixmp
import message_ix
import cx_Oracle
from message_ix_models.util import private_data_path

#from message_data.model.material import data_petro, data_ammonia_new, data_methanol_new
from message_data.tools.post_processing.iamc_report_hackathon import report as reporting
from message_data.tools.utilities import add_globiom

# def main():

#     clone = scen.clone("test", "test")
#     #petro_dict = data_ammonia_new.gen_all_NH3_fert(clone)
#     petro_dict = data_methanol_new.gen_data_methanol_new(clone)
#     print("test")
#     #petro_dict = data_cement.gen_data_cement(scen)
magpie_scens = [
    "MP00BD0BI78",
    "MP00BD0BI74",
    "MP00BD0BI70",
    "MP00BD1BI00",
    #"MP30BD0BI78",
    "MP30BD0BI74",
    "MP30BD0BI70",
    "MP30BD1BI00",
    "MP50BD0BI78",
    "MP50BD0BI74",
    "MP50BD0BI70",
    "MP50BD1BI00",
    "MP76BD0BI70",
    "MP76BD0BI74",
    "MP76BD0BI78",
    "MP76BD1BI00"
]

if __name__ == '__main__':
    #main()
    # import warnings
    #
    #
    from message_ix_models.cli import main
    #
    # mp = ixmp.Platform("ixmp_dev")
    # scen = message_ix.Scenario(mp, "SHAPE_SSP2_v4.1.8", "baseline")
    # reporting(
    #     mp,
    #     scen,
    #     # NB(PNK) this is not an error; .iamc_report_hackathon.report() expects a
    #     #         string containing "True" or "False" instead of an actual bool.
    #     "False",
    #     scen.model,
    #     scen.scenario+"_test",
    #     merge_hist=True,
    #     merge_ts=True,
    #     run_config="materials_run_config.yaml",
    # )

    # import os
    # path_dirs = os.environ.get('PATH').split(os.pathsep)
    #
    # import sys
    # new_path = "C:/Users/maczek/instantclient_21_10"
    # sys.path.append(new_path)
    # cx_Oracle.init_oracle_client(lib_dir=r"C:/Users/maczek/instantclient_21_10")
    # #print(path_dirs)
    # # main(["--url", "ixmp://ixmp_dev/MESSAGEix-Materials/NoPolicy_petro_thesis_2",
    # #      "--local-data", "./data", "material", "debug_module", "--material", "steel"])
    # # main(["--url", "ixmp://ixmp_dev/SHAPE_SSP2_v4.1.8/baseline#1",
    # #      "--local-data", "./data" , "material",  "build", "--tag", "petro_thesis_2"])
    # # main(["--local-data", "./data", "material", "solve" "--scenario_name",
    # #      "NoPolicy_petro_thesis_2" "--add_calibration", True])
    # # main(["--local-data", "C:/Users\maczek\PycharmProjects\message_data", "navigate_industry",
    # #       "--run-reporting-only", "run", "SSP2", "--output-model=MESSAGEix-GLOBIOM 1.1-R12-MAGPIE-MP00BD1BI00",
    # #      "--scenarios", "ENf", "--run_reporting" ,"True"])
    # # exit()
    # main(["--local-data", "C:/Users\maczek\PycharmProjects\message_data", "navigate_industry",
    #       "--run-reporting-only", "run", "SSP2", f'--output-model=MESSAGEix-GLOBIOM 1.1-R12-MAGPIE-MP00BD0BI70',
    #       "--scenarios", "EN-step3", "--run_reporting", "True"])
    # for magpie_scen in magpie_scens[5:]:
    #     print()
    #     print()
    #     try:
    #         main(["--local-data", "C:/Users\maczek\PycharmProjects\message_data", "navigate_industry",
    #               "run", "SSP2", f'--output-model=MESSAGEix-GLOBIOM 1.1-R12-MAGPIE-{magpie_scen}',
    #               "--scenarios", "run_cdlinks_setup", ])
    #     except SystemExit as e:
    #         pass
    #         # if e.code != 0:
    #         #     pass#raise
    #     # else:
    #     #     pass
    # mp = ixmp.Platform("ixmp_dev")
    # scen = message_ix.Scenario(mp, "MESSAGEix-GLOBIOM 1.1-R12-MAGPIE", "baseline_w/o_LU")
    # sc_clone = scen.clone("MESSAGEix-GLOBIOM 1.1-R12-MAGPIE-MP00BD1BI00", "bugfix_debug")
    # add_globiom(
    #     mp,
    #     sc_clone,
    #     "SSP2",
    #     private_data_path(),
    #     2015,
    #     globiom_scenario="noSDG_rcpref",
    #     #regenerate_input_data=True,
    #     #regenerate_input_data_only=True,
    #     #globiom_scenario="no",
    #     #allow_empty_drivers=True,
    #     #add_reporting_data=True,
    #     #config_setup="MAGPIE_MP00BD1BI00",
    #     config_setup="MAGPIE_MP00BI00_OldForSplit",
    #     config_file="magpie_config.yaml"
    # )
    # main(["--local-data", "C:/Users\maczek\PycharmProjects\message_data", "magpie_scp",
    #       "--run-reporting-only", "run", "SSP2", f'--output-model=MESSAGEix-GLOBIOM 1.1-R12-MAGPIE-MP00BD0BI70',
    #       "--scenarios", "EN-step3", "--run_reporting", "True"])
    # main(["--url", "ixmp://ixmp_dev/MESSAGEix-Materials/NoPolicy_petro_thesis_2_macro", "report"])
    #, f'MESSAGEix-GLOBIOM 1.1-R12-MAGPIE-MP00BD0BI70',
    #      "--scenario", "baseline"])
    main(["--url", "ixmp://ixmp_dev/SSP_dev_SSP2_v0.1/baseline",
         "--local-data", "./data" , "material",  "build", "--tag", " v0.1_materials_no_pow_sect"])
