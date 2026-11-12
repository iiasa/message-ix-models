import pandas as pd
import pytest

from message_ix_models.report import report
from message_ix_models.report.legacy.country import (
    GWa_to_EJ,
    _transact_ts,
    closure,
    stat_diff_data,
    write_xlsx,
)
from message_ix_models.testing import bare_res

IDX = dict(node_loc="R12_ABC", year_act=2020)


def test_stat_diff_data() -> None:
    act = pd.DataFrame(
        [
            ["stat_diff_gas_final", "M1", 2.0],
            ["stat_diff_coal_final", "M2", 1.0],
            ["stat_diff_oper_res", "M1", 3.0],
        ],
        columns=["technology", "mode", "lvl"],
    ).assign(**IDX)
    output = pd.DataFrame(
        [["stat_diff_gas_final", "M1", "gas", "final", 1.0]],
        columns=["technology", "mode", "commodity", "level", "value"],
    ).assign(**IDX)
    input = pd.DataFrame(
        [["stat_diff_coal_final", "M2", "coal", "final", 1.0]],
        columns=["technology", "mode", "commodity", "level", "value"],
    ).assign(**IDX)
    rel = pd.DataFrame(
        [["stat_diff_oper_res", "M1", "oper_res", -1.0]],
        columns=["technology", "mode", "relation", "value"],
    ).assign(**IDX)

    result = stat_diff_data(act, input, output, rel, "m", "s", region="R12_GLB")

    assert {"model", "scenario", "region", "variable", "unit", "year", "value"} == set(
        result.columns
    )
    assert 6 == len(result)  # 3 variables x 2 regions
    r = result.set_index(["region", "variable"])["value"]
    # Supply is positive, use negative, in EJ/yr
    assert r["R12_ABC", "Statistical Difference|gas|final"] == 2.0 * GWa_to_EJ
    assert r["R12_ABC", "Statistical Difference|coal|final"] == -1.0 * GWa_to_EJ
    # A relation: activity x coefficient
    assert r["R12_ABC", "Statistical Difference|oper_res"] == -3.0
    # Summed into the given region
    assert r["R12_GLB", "Statistical Difference|gas|final"] == 2.0 * GWa_to_EJ

    # No statistical differences: empty result with the same columns
    e = pd.DataFrame()
    empty = stat_diff_data(e, e, e, e, "m", "s")
    assert empty.empty and set(result.columns) == set(empty.columns)


def test_closure() -> None:
    years = [2020, 2030]
    ts = pd.DataFrame(
        [
            ("Emissions|CO2", 2020, 44.0),
            ("Emissions|CO2", 2030, 1.0),
            ("Emissions|Kyoto Gases", 2020, 44.0),
            ("Emissions|Kyoto Gases", 2030, 44.0),
            ("Final Energy", 2020, 3.0),
            ("Final Energy", 2030, 3.0),
            ("Final Energy|Industry", 2020, 1.0),
            ("Final Energy|Industry", 2030, 1.0),
            ("Final Energy|Transportation", 2020, 2.0),
            ("Final Energy|Transportation", 2030, 2.0),
            ("Final Energy|Electricity", 2020, 3.0),
            ("Final Energy|Electricity", 2030, 2.0),
        ],
        columns=["variable", "year", "value"],
    )
    act = pd.DataFrame(dict(year_act=years, lvl=[12.0, 12.0]))
    emiss = pd.DataFrame(dict(year=years, lvl=[12.0, 12.0]))

    df = closure(ts, act, emiss).set_index(["check", "reference", "year"]).sort_index()

    assert df.loc[("Emissions|CO2", "ACT of CO2_TCE x 44/12", 2020), "ok"]
    assert not df.loc[("Emissions|CO2", "ACT of CO2_TCE x 44/12", 2030), "ok"]
    assert df.loc[("Emissions|Kyoto Gases", "EMISS of TCE x 44/12")].ok.all()
    assert df.loc[("Final Energy", "sum of 2 sectors")].ok.all()
    fuels = df.loc[("Final Energy", "sum of 1 fuels")]
    assert [True, False] == fuels.ok.tolist()
    # Primary Energy is not reported: no part, reported value NaN
    assert not df.loc[("Primary Energy", "sum of 0 fuels")].ok.any()


def test_use_legacy_file(test_context, tmp_path) -> None:
    cfg = test_context.report

    cfg.use_legacy_file("country")
    assert cfg.legacy["use"] is True
    assert "country.yaml" == cfg.legacy["config_file_path"].name

    with pytest.raises(FileNotFoundError, match="Legacy reporting configuration"):
        cfg.use_legacy_file(tmp_path.joinpath("missing"))


def test_report_legacy_country(caplog, request, tmp_path, test_context) -> None:
    """Legacy reporting of a single-country model; dry run only."""
    scenario = bare_res(request, test_context, solved=False)
    test_context.set_scenario(scenario)
    test_context.dry_run = True
    test_context.report.cli_output = tmp_path

    # A file with settings of the "country" file, except using .report.legacy
    path = tmp_path.joinpath("country.yaml")
    path.write_text(
        "module: message_ix_models.report.legacy.iamc_report_hackathon\n"
        "clear_timeseries: true\nmaterials: {region: R12_GLB}\n"
        "country: {region: R12_GLB}\nmerge_hist: true\n"
    )
    test_context.report.use_legacy_file(path)

    report(test_context)

    # Output directory is under the --output path, named for the scenario
    exp = tmp_path.joinpath(f"{scenario.model}_{scenario.scenario}")
    assert any(f"Would report {scenario.url} to {exp}" in m for m in caplog.messages)
    # Nothing written
    assert not exp.exists()


def test_write_xlsx(request, test_context, tmp_path) -> None:
    """The workbook holds all stored time series data, including data added later."""
    scenario = bare_res(request, test_context, solved=False)
    data = pd.DataFrame(
        dict(
            region="World",
            variable="Statistical Difference|gas|secondary",
            unit="EJ/yr",
            year=[2020],
            value=[0.25],
        )
    )
    with _transact_ts(scenario, "Add test data"):
        scenario.add_timeseries(data)

    path = write_xlsx(scenario, tmp_path)

    assert f"{scenario.model}_{scenario.scenario}.xlsx" == path.name
    df = pd.read_excel(path, sheet_name="data")
    assert ["Model", "Scenario", "Region", "Variable", "Unit"] == list(df.columns[:5])
    row = df[df.Variable == "Statistical Difference|gas|secondary"]
    assert [("World", "EJ/yr", 0.25)] == list(zip(row.Region, row.Unit, row[2020]))
