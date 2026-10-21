"""Tests for :mod:`message_ix_models.project.sparccle.workflow`."""

from pathlib import Path

import pytest
import yaml

from message_ix_models.project.sparccle import workflow
from message_ix_models.project.sparccle.workflow import (
    MAX_SCENARIO_NAME,
    _missing_buildings,
    _missing_magicc,
    _missing_rime,
    _resolve_magicc_dir,
    generate,
    load_config,
    name_token,
    validate_inputs,
    validate_names,
)


def test_load_config_missing_required_key_raises(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump({"platform_info": {"name": "ixmp_dev"}}))

    with pytest.raises(ValueError, match="missing required keys"):
        load_config(path)


def test_load_config_fills_defaults(tmp_path: Path) -> None:
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
    assert config["cid"] == {
        "n_runs": None,
        "min_year": None,
        "response_percentile": None,
        "climate_label": None,
    }
    assert config["regions"] == "R12"


def test_missing_magicc_reports_absent_directory(tmp_path: Path) -> None:
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


def test_missing_magicc_reports_absent_xlsx(tmp_path: Path) -> None:
    starters = [{"model": "m", "scenario": "s", "magicc_output_dir": str(tmp_path)}]

    missing = _missing_magicc(starters)

    assert len(missing) == 1
    assert "IAMC_climateassessment" in missing[0]


def test_missing_magicc_empty_when_file_present(tmp_path: Path) -> None:
    (tmp_path / "foo_IAMC_climateassessment.xlsx").touch()
    starters = [{"model": "m", "scenario": "s", "magicc_output_dir": str(tmp_path)}]

    assert _missing_magicc(starters) == []


def test_missing_buildings_empty_for_packaged_ssps() -> None:
    assert _missing_buildings({"SSP1", "SSP2", "SSP3"}) == []


def test_missing_buildings_reports_absent_ssp() -> None:
    missing = _missing_buildings({"SSP9"})

    assert missing
    assert all("SSP9" in m for m in missing)


def test_missing_rime_empty_when_packaged_data_present() -> None:
    assert _missing_rime() == []


def test_validate_inputs_aggregates_all_missing_paths(tmp_path: Path) -> None:
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


def test_validate_inputs_passes_with_all_inputs_present(tmp_path: Path) -> None:
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


def _write_config(tmp_path: Path, magicc_dir: Path) -> str:
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


def test_generate_builds_one_step_set_per_starter(test_context, tmp_path: Path) -> None:
    (tmp_path / "foo_IAMC_climateassessment.xlsx").touch()
    config_path = _write_config(tmp_path, tmp_path)

    wf = generate(test_context, config_path=config_path)

    assert wf.default_key == "all CI"
    for label in ("SSP1/s1", "SSP2/s2"):
        for suffix in ("base", "cooling", "CI_b", "CI_p", "CI_bp"):
            assert f"{label} {suffix}" in wf.graph
    assert len(wf.graph["all CI"]) == 6


STARTER = {
    "model": "SSP_SSP3_v6.6_sp",
    "scenario": "NPiREF_GDP_CI_95_ensemble_2_PHY",
    "ssp": "SSP3",
}


def test_resolve_magicc_dir_root_composes_per_starter() -> None:
    result = _resolve_magicc_dir(
        STARTER,
        magicc_root="/data/magicc",
        magicc_file=None,
        magicc_model_suffix="",
        n_starters=2,
    )
    assert result == ("/data/magicc/SSP_SSP3_v6.6_sp/NPiREF_GDP_CI_95_ensemble_2")


def test_resolve_magicc_dir_model_suffix_appends_to_model_component() -> None:
    result = _resolve_magicc_dir(
        STARTER,
        magicc_root="/data/magicc",
        magicc_file=None,
        magicc_model_suffix="_p95",
        n_starters=2,
    )
    assert result == ("/data/magicc/SSP_SSP3_v6.6_sp_p95/NPiREF_GDP_CI_95_ensemble_2")


def test_resolve_magicc_dir_scenario_without_phy_suffix_preserved() -> None:
    starter = {**STARTER, "scenario": "NPiREF_GDP_CI_95_ensemble_2"}
    result = _resolve_magicc_dir(
        starter,
        magicc_root="/data/magicc",
        magicc_file=None,
        magicc_model_suffix="",
        n_starters=1,
    )
    assert result.endswith("/NPiREF_GDP_CI_95_ensemble_2")


def test_resolve_magicc_dir_file_overrides_root_and_yaml(tmp_path: Path) -> None:
    f = tmp_path / "x_IAMC_climateassessment.xlsx"
    f.touch()
    starter = {**STARTER, "magicc_output_dir": "/should/be/ignored"}
    result = _resolve_magicc_dir(
        starter,
        magicc_root="/also/ignored",
        magicc_file=f,
        magicc_model_suffix="",
        n_starters=1,
    )
    assert result == str(tmp_path)


def test_resolve_magicc_dir_file_rejected_for_multi_starter(tmp_path: Path) -> None:
    f = tmp_path / "x.xlsx"
    f.touch()
    with pytest.raises(ValueError, match="exactly one starter"):
        _resolve_magicc_dir(
            STARTER,
            magicc_root=None,
            magicc_file=f,
            magicc_model_suffix="",
            n_starters=3,
        )


def test_resolve_magicc_dir_yaml_fallback_when_no_flags() -> None:
    starter = {**STARTER, "magicc_output_dir": "/legacy/path"}
    result = _resolve_magicc_dir(
        starter,
        magicc_root=None,
        magicc_file=None,
        magicc_model_suffix="",
        n_starters=1,
    )
    assert result == "/legacy/path"


def test_resolve_magicc_dir_root_overrides_yaml() -> None:
    starter = {**STARTER, "magicc_output_dir": "/should/be/ignored"}
    result = _resolve_magicc_dir(
        starter,
        magicc_root="/data/magicc",
        magicc_file=None,
        magicc_model_suffix="",
        n_starters=1,
    )
    assert result.startswith("/data/magicc/")


def test_resolve_magicc_dir_missing_path_raises() -> None:
    with pytest.raises(ValueError, match="lacks magicc_output_dir"):
        _resolve_magicc_dir(
            STARTER,
            magicc_root=None,
            magicc_file=None,
            magicc_model_suffix="",
            n_starters=1,
        )


# The token that keeps two response arms from sharing one scenario name.


def test_name_token_absent_label_leaves_names_unchanged() -> None:
    # A config that does not opt in must keep producing the names it always
    # has, so no already-delivered row moves.
    assert name_token({"climate_label": None, "response_percentile": None}) == ""
    assert name_token({"climate_label": None, "response_percentile": 90}) == ""


def test_name_token_mean_response_reads_as_E() -> None:
    token = name_token({"climate_label": "c95", "response_percentile": None})
    assert token == "_c95_iE"


def test_name_token_percentile_response_carries_its_number() -> None:
    token = name_token({"climate_label": "c95", "response_percentile": 90})
    assert token == "_c95_i90"


def test_name_token_two_arms_of_one_starter_get_different_names() -> None:
    # The defect this exists to prevent: the same starter built at two
    # response percentiles wrote one name, and whichever ran last won.
    starter = "INDC2030i_600f_GDP_CI_95_ensemble_1_PHY_cooling_CI_bp"
    expectation = starter + name_token(
        {"climate_label": "c95", "response_percentile": None}
    )
    tail = starter + name_token({"climate_label": "c95", "response_percentile": 90})
    assert expectation != tail


@pytest.mark.parametrize(
    "filename, expected",
    [
        ("scenario_config.yaml", ""),
        ("scenario_config_p95.yaml", "_c95_iE"),
        ("scenario_config_sts1_p95.yaml", "_c95_i90"),
    ],
)
def test_name_token_packaged_configs(filename: str, expected: str) -> None:
    path = Path(workflow.__file__).parent / filename
    assert name_token(load_config(path)["cid"]) == expected


PACKAGED_CONFIGS = [
    "scenario_config.yaml",
    "scenario_config_p95.yaml",
    "scenario_config_sts1_p95.yaml",
]


# Names too long for the database must fail before anything is cloned.


def _names_config(scenario: str) -> dict:
    return {"starters": [{"scenario": scenario}]}


def test_validate_names_name_within_the_limit_passes() -> None:
    # 39 characters plus the longest suffix and a 11-character token is the
    # widest thing the shipping p95 configs ask for.
    validate_names(
        _names_config("INDC2030i_600f_GDP_CI_95_ensemble_1_PHY"), "_" + "x" * 10
    )


def test_validate_names_over_long_name_names_itself_and_its_length() -> None:
    with pytest.raises(ValueError) as exc:
        validate_names(
            _names_config("INDC2030i_600f_GDP_CI_95_ensemble_1_PHY"),
            "_climp95_imp90",
        )
    message = str(exc.value)
    over_long = "INDC2030i_600f_GDP_CI_95_ensemble_1_PHY_cooling_CI_bp_climp95_imp90"
    assert over_long in message
    assert "67 characters" in message
    assert str(MAX_SCENARIO_NAME) in message


def test_validate_names_every_suffix_is_checked() -> None:
    # A token long enough that only the shortest suffix survives: the
    # buildings-only name must still be reported.
    with pytest.raises(ValueError) as exc:
        validate_names(_names_config("x" * 55), "_" + "y" * 5)
    assert "_CI_b" in str(exc.value)


@pytest.mark.parametrize("filename", PACKAGED_CONFIGS)
def test_validate_names_packaged_configs_fit(filename: str) -> None:
    config = load_config(Path(workflow.__file__).parent / filename)
    validate_names(config, name_token(config["cid"]))


def test_cli_help_lists_run(mix_models_cli) -> None:
    """The ``sparccle`` group is registered and exposes ``run``."""
    result = mix_models_cli.assert_exit_0(["sparccle", "--help"])
    assert "run" in result.output


def test_cli_run_help_lists_options(mix_models_cli) -> None:
    """``sparccle run`` exposes the MAGICC input options."""
    result = mix_models_cli.assert_exit_0(["sparccle", "run", "--help"])
    for option in ("--config", "--magicc-root", "--magicc-file"):
        assert option in result.output


def test_cli_run_missing_config_fails(mix_models_cli, tmp_path: Path) -> None:
    """A nonexistent ``--config`` path is rejected before any work starts."""
    missing = tmp_path / "absent.yaml"
    result = mix_models_cli.invoke(["sparccle", "run", f"--config={missing}"])
    assert result.exit_code != 0
    assert "does not exist" in result.output
