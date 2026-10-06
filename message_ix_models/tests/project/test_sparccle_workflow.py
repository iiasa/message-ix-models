"""Tests for project.sparccle.workflow — config loading, preflight, graph shape."""

import pytest
import yaml

from message_ix_models.project.sparccle.workflow import (
    _missing_buildings,
    _missing_magicc,
    _missing_rime,
    generate,
    load_config,
    validate_inputs,
)


def test_load_config_missing_required_key_raises(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump({"platform_info": {"name": "ixmp_dev"}}))

    with pytest.raises(ValueError, match="missing required keys"):
        load_config(path)


def test_load_config_fills_defaults(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "platform_info": {"name": "ixmp_dev"},
                "starters": [],
                "cooling": {},
            }
        )
    )

    config = load_config(path)

    assert config["cooling"] == {"rcps": "no_climate", "rels": "low"}
    assert config["cid"] == {"n_runs": None, "min_year": None}
    assert config["regions"] == "R12"


def test_missing_magicc_reports_absent_directory(tmp_path):
    starters = [
        {
            "model": "m",
            "scenario": "s",
            "magicc_output_dir": str(tmp_path / "does-not-exist"),
        }
    ]

    missing = _missing_magicc(starters)

    assert len(missing) == 1
    assert "m/s" in missing[0]


def test_missing_magicc_reports_absent_xlsx(tmp_path):
    starters = [{"model": "m", "scenario": "s", "magicc_output_dir": str(tmp_path)}]

    missing = _missing_magicc(starters)

    assert len(missing) == 1
    assert "IAMC_climateassessment" in missing[0]


def test_missing_magicc_empty_when_file_present(tmp_path):
    (tmp_path / "foo_IAMC_climateassessment.xlsx").touch()
    starters = [{"model": "m", "scenario": "s", "magicc_output_dir": str(tmp_path)}]

    assert _missing_magicc(starters) == []


def test_missing_buildings_empty_for_packaged_ssps():
    assert _missing_buildings({"SSP1", "SSP2", "SSP3"}) == []


def test_missing_buildings_reports_absent_ssp():
    missing = _missing_buildings({"SSP9"})

    assert missing
    assert all("SSP9" in m for m in missing)


def test_missing_rime_empty_when_packaged_data_present():
    assert _missing_rime() == []


def test_validate_inputs_aggregates_all_missing_paths(tmp_path):
    config = {
        "starters": [
            {
                "model": "m",
                "scenario": "s",
                "ssp": "SSP9",
                "magicc_output_dir": str(tmp_path / "missing"),
            }
        ]
    }

    with pytest.raises(FileNotFoundError) as exc_info:
        validate_inputs(config)

    message = str(exc_info.value)
    assert "m/s" in message
    assert "SSP9" in message


def test_validate_inputs_passes_with_all_inputs_present(tmp_path):
    (tmp_path / "foo_IAMC_climateassessment.xlsx").touch()
    config = {
        "starters": [
            {
                "model": "m",
                "scenario": "s",
                "ssp": "SSP1",
                "magicc_output_dir": str(tmp_path),
            }
        ]
    }

    validate_inputs(config)


def _write_config(tmp_path, magicc_dir) -> str:
    path = tmp_path / "config.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "platform_info": {"name": "ixmp_dev"},
                "regions": "R12",
                "starters": [
                    {
                        "model": "m",
                        "scenario": "s1",
                        "ssp": "SSP1",
                        "magicc_output_dir": str(magicc_dir),
                    },
                    {
                        "model": "m",
                        "scenario": "s2",
                        "ssp": "SSP2",
                        "magicc_output_dir": str(magicc_dir),
                    },
                ],
                "cooling": {"rcps": "no_climate", "rels": "low"},
                "cid": {"n_runs": None, "min_year": None},
            }
        )
    )
    return str(path)


def test_generate_builds_one_step_set_per_starter(test_context, tmp_path):
    (tmp_path / "foo_IAMC_climateassessment.xlsx").touch()
    config_path = _write_config(tmp_path, tmp_path)

    wf = generate(test_context, config_path=config_path)

    assert wf.default_key == "all CI"
    for label in ("SSP1/s1", "SSP2/s2"):
        for suffix in ("base", "cooling", "CI_b", "CI_p", "CI_bp"):
            assert f"{label} {suffix}" in wf.graph
    assert len(wf.graph["all CI"]) == 6
