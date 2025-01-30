# tests for the GDP implementation

from message_ix_models.util import private_data_path

from message_data.projects.GDP_climate_shocks.call_climate_processor import (
    read_magicc_output,
)
from message_data.projects.GDP_climate_shocks.util import load_gdp_data


def test_load_gdp_data():
    gdp_df = load_gdp_data()
    assert gdp_df.shape[0] > 0
    assert gdp_df.shape[1] > 0
    assert "region" in gdp_df.columns
    assert "iso3" in gdp_df.columns
    assert "year" in gdp_df.columns
    assert "value" in gdp_df.columns
    assert "unit" in gdp_df.columns
    assert "model" in gdp_df.columns
    assert "scenario" in gdp_df.columns
    assert "variable" in gdp_df.columns
    assert "variable" in gdp_df.columns


# regional_gdp_impacts
# def test_output_type():
#     result = regional_gdp_impacts("scenario1", "Waidelich", 1, "SSP2")
#     assert isinstance(result, pd.DataFrame)
#     assert list(result.columns) == ["node", "year", "perc_change_sum"]

# def test_known_input_output():
#     # replace with actual known input and output
#     known_input = ("scenario1", "Waidelich", 1, "SSP1")
#     known_output = pd.DataFrame(
#         {"node": ["node1"], "year": [2020], "perc_change_sum": [0.1]}
#     )
#     result = regional_gdp_impacts(*known_input)
#     # filter year == 2020
#     result = result[result.year == 2020]
#     pd.testing.assert_frame_equal(result, known_output)

# def test_invalid_damage_model():
#     with pytest.raises(AssertionError):
#         regional_gdp_impacts("scenario1", "InvalidModel", 1, "SSP1")


# Magic
def test_read_magicc_output():
    # create a fake dataframe
    df = pd.DataFrame(
        {
            "Variable": [
                "AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3|50.0th Percentile"
            ],
            "Region": ["World"],
            "2100": [1.5],
        }
    )

    # save the dataframe to a file
    df.to_excel(
        private_data_path().parent
        / "reporting_output"
        / "magicc_output"
        / "test_magicc.xlsx"
    )

    # test the function
    val = read_magicc_output("test")
    assert val == 1.5
    # remove the file
    (
        private_data_path().parent
        / "reporting_output"
        / "magicc_output"
        / "test_magicc.xlsx"
    ).unlink()
