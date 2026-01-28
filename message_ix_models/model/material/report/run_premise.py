import constants as c
import ixmp
import message_ix
import premise
from message_ix import Reporter
from premise import merge_reports, query_magicc_data

from message_ix_models import Context
from message_ix_models.report import prepare_reporter

if __name__ == "__main__":
    ctx = Context()
    scens = c.low_overshoot  # med_and_medlow low_emi + high +low_overshoot + very_low
    mp = ixmp.Platform("ixmp_dev")
    for model_name, scen_name in scens:
        print(f"Running premise for {model_name} - {scen_name}")
        scen = message_ix.Scenario(mp, model_name, scen_name)
        rep = Reporter.from_scenario(scen)
        prepare_reporter(ctx, reporter=rep)
        df = premise.run(rep, scen, scen.model, scen.scenario)
        df = df.filter(unit=["dimensionless", "", None], keep=False).rename(
            mapping={"unit": {"Mt / a": "Mt/yr", "EJ / a": "EJ/yr"}}
        )
        df.to_excel(f"{model_name}_{scen_name}.xlsx")
        del scen
    query_magicc_data()
    merge_reports()
