import ixmp
import message_ix
import data_scp
from message_data.tools.utilities import add_globiom
from message_ix_models.util import private_data_path

import message_data.tools.post_processing.iamc_report_hackathon


magpie_scens = [
    "MP00BD0BI78",
    "MP00BD0BI74",
    "MP00BD0BI70",
    "MP00BD1BI00",
    "MP30BD0BI78",
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


def build_magpie_baseline_from_r12_base(mp, scen_base, magpie_scen):
    scen = scen_base.clone(scen_base.model+"-"+magpie_scen, "baseline")
    if scen.has_solution():
        scen.remove_solution()
    add_globiom(
        mp,
        scen,
        "SSP2",
        private_data_path(),
        2015,
        globiom_scenario="noSDG_rcpref",
        regenerate_input_data=True,
        #regenerate_input_data_only=True,
        #allow_empty_drivers=True,
        add_reporting_data=True,
        config_setup="MAGPIE_"+magpie_scen
    )
    scen.set_as_default()
    return scen


def solve_mp_baseline():
    mp = ixmp.Platform("ixmp_dev")
    for magpie_scen in magpie_scens[5:]:
        scen = message_ix.Scenario(mp, f"MESSAGEix-GLOBIOM 1.1-R12-MAGPIE-{magpie_scen}", "baseline")
        scen.solve(model="MESSAGE-MACRO")


def run_reporting():
    mp = ixmp.Platform("ixmp_dev")
    for magpie_scen in magpie_scens:
        print()
        print(f"starting reporting of {magpie_scen}")
        print()
        scen = message_ix.Scenario(mp, f"MESSAGEix-GLOBIOM 1.1-R12-MAGPIE-{magpie_scen}", "baseline")
        if scen.has_solution():
            message_data.tools.post_processing.iamc_report_hackathon.report(
                mp,
                scen,
                # NB(PNK) this is not an error; .iamc_report_hackathon.report() expects a
                #         string containing "True" or "False" instead of an actual bool.
                "False",
                scen.model,
                scen.scenario,
                merge_hist=True,
                merge_ts=True,
                # run_config="materials_run_config.yaml",
            )
        del scen


def main():
    mp = ixmp.Platform("ixmp_dev")
    scen_base = message_ix.Scenario(mp, "MESSAGEix-GLOBIOM 1.1-R12-MAGPIE", "baseline_w/o_LU")

    for magpie_scen in magpie_scens[-1:]:
        print(f"read-in of {magpie_scen} matrix started")
        scen = build_magpie_baseline_from_r12_base(mp, scen_base, magpie_scen)
        if "MP00" not in magpie_scen:
            print(f"adding SCP model to {magpie_scen}")
            scp_dict = data_scp.gen_data_scp(scen)
            scen.check_out()
            data_scp.add_scp_sets(scen)
            for k,v in scp_dict.items():
                scen.add_par(k, v)
            scen.commit("add SCP tecs")
        scen.set_as_default()


if __name__ == '__main__':
    run_reporting()