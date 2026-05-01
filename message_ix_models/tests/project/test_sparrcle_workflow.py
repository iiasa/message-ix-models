"""Tests for project.sparrcle.workflow — Workflow construction + actions."""

from types import SimpleNamespace

from message_ix_models.project.sparrcle import workflow as wf_mod


def _config():
    return {
        "platform_info": {"name": "ixmp_dev"},
        "starters": [
            {
                "model": "SSP_SSP1_v6.5_sp",
                "scenario": "BASE_SSP1",
                "ssp": "SSP1",
                "magicc_output_dir": "/fake/ssp1",
            },
            {
                "model": "SSP_SSP2_v6.5_sp",
                "scenario": "BASE_SSP2",
                "ssp": "SSP2",
                "magicc_output_dir": "/fake/ssp2",
            },
        ],
        "cooling": {"rcps": "no_climate", "rels": "low"},
        "regions": "R12",
        "cid": {"n_runs": 3, "min_year": 2045},
    }


def _build_wf(monkeypatch, tmp_path, context):
    config_path = tmp_path / "sparrcle.yaml"
    config_path.write_text("# stub\n")
    monkeypatch.setattr(wf_mod, "load_config", lambda _p: _config())
    monkeypatch.setattr(wf_mod, "validate_inputs", lambda _c: None)
    return wf_mod.generate(context, config_path=config_path)


# ---------------------------------------------------------------------------
# generate()
# ---------------------------------------------------------------------------


def test_generate_step_names_and_dependencies(monkeypatch, tmp_path):
    wf = _build_wf(monkeypatch, tmp_path, context=SimpleNamespace())
    # Expected step set: per starter (base, cooling, CI_b, CI_p, CI_bp)
    # plus aggregator and the implicit "context".
    expected_steps = {
        "SSP1/BASE_SSP1 base",
        "SSP1/BASE_SSP1 cooling",
        "SSP1/BASE_SSP1 CI_b",
        "SSP1/BASE_SSP1 CI_p",
        "SSP1/BASE_SSP1 CI_bp",
        "SSP2/BASE_SSP2 base",
        "SSP2/BASE_SSP2 cooling",
        "SSP2/BASE_SSP2 CI_b",
        "SSP2/BASE_SSP2 CI_p",
        "SSP2/BASE_SSP2 CI_bp",
        "all CI",
    }
    assert expected_steps <= set(wf.keys())
    assert wf.default_key == "all CI"


def test_generate_variant_bases(monkeypatch, tmp_path):
    wf = _build_wf(monkeypatch, tmp_path, context=SimpleNamespace())
    # genno graph entries are tuples (callable, *deps); deps[1] is `context`,
    # deps[2] is the base step.
    assert wf.graph["SSP1/BASE_SSP1 CI_b"][2] == "SSP1/BASE_SSP1 base"
    assert wf.graph["SSP1/BASE_SSP1 CI_p"][2] == "SSP1/BASE_SSP1 cooling"
    assert wf.graph["SSP1/BASE_SSP1 CI_bp"][2] == "SSP1/BASE_SSP1 cooling"


# ---------------------------------------------------------------------------
# phase1_cooling
# ---------------------------------------------------------------------------


def test_phase1_cooling_subprocess_invocation_and_load(monkeypatch):
    ran = {}

    def fake_run(cmd, check):
        ran["cmd"] = cmd
        ran["check"] = check
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(wf_mod.subprocess, "run", fake_run)

    loaded: list[tuple[str, str]] = []

    class FakeScenario:
        def __init__(self, platform, model, scenario):
            loaded.append((model, scenario))
            self.platform = platform
            self.model = model
            self.scenario = scenario

    monkeypatch.setattr("message_ix.Scenario", FakeScenario)

    scen_in = SimpleNamespace(
        platform=SimpleNamespace(name="ixmp_dev"),
        model="SSP_SSP2_v6.5_sp",
        scenario="BASE_SSP2",
    )
    result = wf_mod.phase1_cooling(
        SimpleNamespace(),
        scen_in,
        ssp="SSP2",
        regions="R12",
        rcps="no_climate",
        rels="low",
    )
    # Subprocess invoked with the expected URL
    assert "--url" in ran["cmd"]
    url_idx = ran["cmd"].index("--url") + 1
    assert ran["cmd"][url_idx] == "ixmp://ixmp_dev/SSP_SSP2_v6.5_sp/BASE_SSP2"
    assert "cooling" in ran["cmd"]
    assert ran["check"] is True
    # Returned scenario is the produced {base}_cooling
    assert (result.model, result.scenario) == (
        "SSP_SSP2_v6.5_sp",
        "BASE_SSP2_cooling",
    )
    assert loaded == [("SSP_SSP2_v6.5_sp", "BASE_SSP2_cooling")]
