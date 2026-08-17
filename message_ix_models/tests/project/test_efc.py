from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace

import pandas as pd
import pytest

from message_ix_models.model.bmt import config as bmt_config
from message_ix_models.project.efc import workflow
from message_ix_models.project.efc.workflow import (
    METH_H2_CO2_RELATIONS,
    _meth_h2_co2_rows,
    _remove_meth_h2_co2_relations,
)

HYWAY_TECHS = {
    "h2_elec_alk",
    "h2_elec_pem",
    "h2_elec_soe",
    "h2_pyro_elec",
    "h2_ct",
}


class ReportingScenario:
    def __init__(self, events: list[str], *, transport: bool = True) -> None:
        self.events = events
        self.transport = transport
        self.model = "model"
        self.scenario = "scenario"
        self.platform = object()

    def par(self, name, filters=None):
        assert name == "demand"
        assert filters == {"commodity": "transport pax UREAM"}
        self.events.append("demand-check")
        return pd.DataFrame([{"value": 1.0}]) if self.transport else pd.DataFrame()

    def check_out(self, timeseries_only=False):
        assert timeseries_only
        self.events.append("check-out")

    def commit(self, message):
        self.events.append(f"commit:{message}")

    def set(self, name):
        assert name == "technology"
        self.events.append("technology-check")
        return pd.Series(sorted(HYWAY_TECHS))

    def add_timeseries(self, data):
        assert "value" in data.columns
        self.events.append("add-timeseries")


class IamData:
    def timeseries(self):
        return pd.DataFrame({"value": [1.0]})


def _stub_reporters(monkeypatch, events: list[str]) -> None:
    legacy = ModuleType("message_data.tools.post_processing.iamc_report_hackathon")
    setattr(legacy, "report", lambda **kwargs: events.append("legacy"))
    post_processing = ModuleType("message_data.tools.post_processing")
    setattr(post_processing, "iamc_report_hackathon", legacy)
    tools = ModuleType("message_data.tools")
    setattr(tools, "post_processing", post_processing)
    message_data = ModuleType("message_data")
    setattr(message_data, "tools", tools)
    monkeypatch.setitem(sys.modules, "message_data", message_data)
    monkeypatch.setitem(sys.modules, "message_data.tools", tools)
    monkeypatch.setitem(
        sys.modules,
        "message_data.tools.post_processing",
        post_processing,
    )
    monkeypatch.setitem(
        sys.modules,
        "message_data.tools.post_processing.iamc_report_hackathon",
        legacy,
    )

    from message_ix.report import Reporter

    from message_ix_models.model.material.report import run_reporting
    from message_ix_models.report.hydrogen import h2_reporting

    def reporter_from_scenario(scenario: object) -> object:
        events.append("reporter")
        return object()

    def run_sectoral_reporting(*args: object, **kwargs: object) -> IamData:
        events.append("genno")
        return IamData()

    monkeypatch.setattr(
        run_reporting,
        "run",
        lambda scenario, region, upload_ts: events.append("materials"),
    )
    monkeypatch.setattr(Reporter, "from_scenario", reporter_from_scenario)
    monkeypatch.setattr(
        h2_reporting,
        "run_sectoral_reporting",
        run_sectoral_reporting,
    )
    monkeypatch.setattr(
        workflow,
        "_write_report_xlsx",
        lambda scenario: events.append("xlsx"),
    )


def test_configure_context(monkeypatch):
    calls = []
    context = SimpleNamespace(model=SimpleNamespace())
    monkeypatch.setattr(
        bmt_config,
        "apply_bmt_config",
        lambda value: calls.append(value),
    )

    workflow.configure_context(context)

    assert context.ssp == "SSP2"
    assert context.model.regions == "R12"
    assert calls == [context]


def test_generate_uses_shared_context_configuration(monkeypatch):
    events = []

    class Workflow:
        def __init__(self, context):
            events.append(("workflow", context))

        def add_step(self, name, *args, **kwargs):
            return name

    context = SimpleNamespace()
    monkeypatch.setattr(workflow, "Workflow", Workflow)
    monkeypatch.setattr(
        workflow,
        "configure_context",
        lambda value: events.append(("configure", value)),
    )

    workflow.generate(context)

    assert events == [("workflow", context), ("configure", context)]


def test_report_runs_complete_order(monkeypatch):
    events = []
    scenario = ReportingScenario(events)
    _stub_reporters(monkeypatch, events)
    monkeypatch.setattr(
        workflow,
        "_run_transport_report",
        lambda context, scenario: events.append("transport") or scenario,
    )

    result = workflow.report(SimpleNamespace(), scenario)

    assert result is scenario
    assert events == [
        "demand-check",
        "transport",
        "check-out",
        "materials",
        "commit:Add materials reporting",
        "legacy",
        "technology-check",
        "reporter",
        "genno",
        "check-out",
        "add-timeseries",
        "commit:Add Genno sectoral reporting",
        "xlsx",
    ]


def test_report_skips_transport_only_without_transport_demand(monkeypatch):
    events = []
    scenario = ReportingScenario(events, transport=False)
    _stub_reporters(monkeypatch, events)
    monkeypatch.setattr(
        workflow,
        "_run_transport_report",
        lambda context, scenario: pytest.fail("transport reporter was called"),
    )

    workflow.report(SimpleNamespace(), scenario)

    assert "materials" in events
    assert "legacy" in events
    assert "genno" in events
    assert "xlsx" in events


def test_report_propagates_transport_failure(monkeypatch):
    events = []
    scenario = ReportingScenario(events)
    _stub_reporters(monkeypatch, events)

    def fail(context, scenario):
        events.append("transport")
        raise RuntimeError("transport configuration failed")

    monkeypatch.setattr(workflow, "_run_transport_report", fail)

    with pytest.raises(RuntimeError, match="transport configuration failed"):
        workflow.report(SimpleNamespace(), scenario)

    assert events == ["demand-check", "transport"]


def _relation_rows() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "relation": [
                "CO2_Emission",
                "CO2_Emission_Global_Total",
                "meth_exp_limit",
            ],
            "node_rel": ["R12_AFR"] * 3,
            "year_rel": [2030] * 3,
            "node_loc": ["R12_AFR"] * 3,
            "technology": ["meth_h2"] * 3,
            "year_act": [2030] * 3,
            "mode": ["fuel_fic"] * 3,
            "value": [0.549, 0.549, 1.0],
            "unit": ["???"] * 3,
        }
    )


class RelationScenario:
    def __init__(self, rows: pd.DataFrame) -> None:
        self.rows = rows
        self.removed = pd.DataFrame()

    def par(self, name: str, filters: dict[str, object]) -> pd.DataFrame:
        assert name == "relation_activity"
        result = self.rows
        for column, values in filters.items():
            values = values if isinstance(values, list) else [values]
            result = result[result[column].isin(values)]
        return result

    def remove_par(self, name: str, rows: pd.DataFrame) -> None:
        assert name == "relation_activity"
        self.removed = rows.copy()
        self.rows = self.rows.drop(rows.index)


def test_select_meth_h2_co2_rows_preserves_other_relations() -> None:
    selected = _meth_h2_co2_rows(_relation_rows())

    assert set(selected["relation"]) == set(METH_H2_CO2_RELATIONS)
    assert len(selected) == 2


def test_select_meth_h2_co2_rows_rejects_wrong_coefficient() -> None:
    rows = _relation_rows()
    rows.loc[0, "value"] = -0.549

    with pytest.raises(RuntimeError, match="Unexpected.*-0.549"):
        _meth_h2_co2_rows(rows)


def test_remove_meth_h2_co2_relations() -> None:
    scenario = RelationScenario(_relation_rows())

    _remove_meth_h2_co2_relations(scenario)  # type: ignore[arg-type]

    assert set(scenario.removed["relation"]) == set(METH_H2_CO2_RELATIONS)
    assert scenario.rows["relation"].tolist() == ["meth_exp_limit"]


def test_remove_meth_h2_co2_relations_is_noop_when_clean() -> None:
    scenario = RelationScenario(_relation_rows().iloc[2:].copy())

    _remove_meth_h2_co2_relations(scenario)  # type: ignore[arg-type]

    assert scenario.removed.empty
    assert scenario.rows["relation"].tolist() == ["meth_exp_limit"]
