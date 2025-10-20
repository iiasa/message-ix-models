from typing import TYPE_CHECKING

import pytest
from message_ix import make_df

from message_ix_models.project.ssp.script.util.functions import add_ccs_setup
from message_ix_models.testing import bare_res

if TYPE_CHECKING:
    from message_ix import Scenario
    from pytest import FixtureRequest

    from message_ix_models import Context


@pytest.fixture
def scenario(request: "FixtureRequest", test_context: "Context") -> "Scenario":
    test_context.model.regions = "R12"
    return bare_res(request, test_context, solved=False)


pytestmark = pytest.mark.usefixtures("ssp_user_data")


def test_add_ccs_setup(scenario: "Scenario") -> None:
    sets = [
        ("relation", "CO2_PtX_trans_disp_split"),
        ("relation", "co2_trans_disp"),
        ("relation", "bco2_trans_disp"),
        ("technology", "co2_tr_dis"),
        ("technology", "bco2_tr_dis"),
        ("mode", "M1"),
        ("mode", "M2"),
        ("mode", "feedstock"),
        ("mode", "fuel"),
        ("mode", "M3"),
        ("technology", "biomass_NH3_ccs"),
        ("technology", "meth_bio_ccs"),
        ("technology", "bf_ccs_steel"),
        ("technology", "clinker_dry_ccs_cement"),
        ("technology", "clinker_wet_ccs_cement"),
        ("technology", "coal_NH3_ccs"),
        ("technology", "dri_gas_ccs_steel"),
        ("technology", "fueloil_NH3_ccs"),
        ("technology", "gas_NH3_ccs"),
        ("node", "R12_GLB"),
    ]
    ccs_techs = [
        # BECCS
        "bio_istig_ccs",
        "biomass_NH3_ccs",
        "bio_ppl_co2scr",
        "eth_bio_ccs",
        "meth_bio_ccs",
        "h2_bio_ccs",
        "liq_bio_ccs",
        # Fossil and Industrial CCS
        "bf_ccs_steel",
        "c_ppl_co2scr",
        "clinker_dry_ccs_cement",
        "clinker_wet_ccs_cement",
        "coal_adv_ccs",
        "coal_NH3_ccs",
        "dri_gas_ccs_steel",
        "fueloil_NH3_ccs",
        "g_ppl_co2scr",
        "gas_cc_ccs",
        "gas_NH3_ccs",
        "h2_coal_ccs",
        "h2_smr_ccs",
        "igcc_ccs",
        "meth_coal_ccs",
        "meth_ng_ccs",
        "syn_liq_ccs",
        # DACCS
        "dac_lt",
        "dac_hte",
        "dac_htg",
    ]
    scenario.platform.add_unit("Mt C/yr")
    with scenario.transact():
        for set, element in sets:
            scenario.add_set(set, element)
        scenario.add_par(
            "technical_lifetime",
            make_df(
                "technical_lifetime",
                year_vtg=2030,
                value=30,
                unit="???",
                node_loc="R12_NAM",
                technology=ccs_techs,
            ),
        )
    add_ccs_setup(scenario, "SSP2")
