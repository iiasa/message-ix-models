from unittest.mock import MagicMock, patch

import pytest

import message_ix_models.project.GDP_climate_shocks.cli as cli

# --- Fixtures ---


@pytest.fixture
def fake_scenario():
    """Return a minimal fake scenario object with necessary methods."""
    sc = MagicMock()
    sc.model = "MODEL"
    sc.scenario = "SCEN"
    sc.platform = MagicMock()
    sc.clone.return_value = sc
    sc.solve.return_value = None
    sc.set_as_default.return_value = None
    return sc


# --- Tests ---


@pytest.mark.parametrize("shift_year", [None, 2025])
def test_run_initial_scenario_if_needed_existing_file(
    fake_scenario, shift_year, tmp_path
):
    """Test that existing MAGICC output skips running the scenario."""
    # Create a fake MAGICC file
    magicc_dir = tmp_path / "reporting_output" / "magicc_output"
    magicc_dir.mkdir(parents=True)
    file_path = magicc_dir / "MODEL_SCEN_0_magicc.xlsx"
    file_path.write_text("dummy")

    with patch(
        "message_ix_models.project.GDP_climate_shocks.cli.private_data_path"
    ) as mock_path:
        mock_path.return_value.parent = tmp_path

        cli.run_initial_scenario_if_needed(
            mp=fake_scenario.platform,
            sc_ref=fake_scenario,
            model_name_clone="CLONE",
            scenario="SCEN",
            shift_year=shift_year,
            sc_str_rime="MODEL_SCEN",
        )

    # If the file exists, clone/solve/processing should NOT be called
    fake_scenario.clone.assert_not_called()


@pytest.mark.parametrize(
    "override_values", [(None, None, None, None, None, None, None, None)]
)
def test_load_and_override_config(tmp_path, override_values):
    """Test loading config and overriding values."""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(
        "model_name: MODEL\n"
        "model_name_clone: CLONE\n"
        "ssp: SSP2\n"
        "scens_ref: SCEN_REF\n"
        "damage_model: DM\n"
        "percentiles: 50\n"
        "shift_year: 2025\n"
        "regions: [R12]\n"
        "rime_path: /tmp/rime\n"
    )

    (
        cfg,
        model_name,
        model_name_clone,
        ssp,
        scens_ref,
        damage_model,
        percentiles,
        shift_year,
        region,
        rime_path,
    ) = cli.load_and_override_config(str(cfg_file), *override_values)

    assert cfg["model_name"] == "MODEL"
    assert region == "R12"  # always first element
