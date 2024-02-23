from message_ix_models.tools.costs.config import Config
from message_ix_models.tools.costs.projections import create_cost_projections


def test_create_cost_projections() -> None:
    cfg = Config(node="R11", scenario="SSP2")

    energy_gdp_r11_message = create_cost_projections(cfg)

    msg_inv = energy_gdp_r11_message["inv_cost"]
    msg_fix = energy_gdp_r11_message["fix_cost"]

    # Assert that all R11 regions are present in both inv and fix
    reg_r11 = [
        "R11_AFR",
        "R11_CPA",
        "R11_EEU",
        "R11_FSU",
        "R11_LAM",
        "R11_MEA",
        "R11_NAM",
        "R11_PAO",
        "R11_PAS",
        "R11_SAS",
        "R11_WEU",
    ]
    assert bool(all(i in msg_inv.node_loc.unique() for i in reg_r11)) is True
    assert bool(all(i in msg_fix.node_loc.unique() for i in reg_r11)) is True

    # Assert that key energy technologies are present in both inv and fix
    tech_energy = ["coal_ppl", "gas_ppl", "wind_ppl", "solar_pv_ppl"]
    assert bool(all(i in msg_inv.technology.unique() for i in tech_energy)) is True
    assert bool(all(i in msg_fix.technology.unique() for i in tech_energy)) is True

    # Assert that columns needed for MESSAGE input are present
    columns_inv = ["node_loc", "technology", "year_vtg", "value"]
    assert bool(all(i in msg_inv.columns for i in columns_inv)) is True
    columns_fix = ["node_loc", "technology", "year_vtg", "year_act", "value"]
    assert bool(all(i in msg_fix.columns for i in columns_fix)) is True

    cfg = Config(
        module="materials", method="convergence", scenario="SSP2", format="iamc"
    )

    materials_converge_r12_iamc = create_cost_projections(cfg)

    iamc_inv = materials_converge_r12_iamc["inv_cost"]
    iamc_fix = materials_converge_r12_iamc["fix_cost"]

    # Assert that all R12 regions are present in both inv and fix
    reg_r12 = [
        "R12_AFR",
        "R12_CHN",
        "R12_EEU",
        "R12_FSU",
        "R12_LAM",
        "R12_MEA",
        "R12_NAM",
        "R12_PAO",
        "R12_PAS",
        "R12_RCPA",
        "R12_SAS",
        "R12_WEU",
    ]
    assert bool(all(i in iamc_inv.Region.unique() for i in reg_r12)) is True
    assert bool(all(i in iamc_fix.Region.unique() for i in reg_r12)) is True

    # Assert that key materials technologies are present in both inv and fix
    tech_materials_inv = [
        "Capital Cost|Electricity|MTO_petro",
        "Capital Cost|Electricity|biomass_NH3",
        "Capital Cost|Electricity|furnace_foil_steel",
    ]

    tech_materials_fix = [
        "OM Cost|Electricity|MTO_petro|Vintage=2020",
        "OM Cost|Electricity|biomass_NH3|Vintage=2050",
        "OM Cost|Electricity|furnace_foil_steel|Vintage=2090",
    ]

    assert (
        bool(all(i in iamc_inv.Variable.unique() for i in tech_materials_inv)) is True
    )
    assert (
        bool(all(i in iamc_fix.Variable.unique() for i in tech_materials_fix)) is True
    )
