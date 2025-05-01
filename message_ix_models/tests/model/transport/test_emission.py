import numpy as np
import pandas as pd
import pytest
from message_ix import make_df

from message_ix_models.model.transport import Config, DataSourceConfig, build, testing
from message_ix_models.model.transport.emission import ef_for_input, get_emissions_data
from message_ix_models.model.transport.testing import MARK
from message_ix_models.util import broadcast, same_node


@build.get_computer.minimum_version
@MARK[10]
def test_ef_for_input(test_context):
    # Generate a test "input" data frame
    _, info = testing.configure_build(test_context, regions="R11", years="B")
    years = info.yv_ya
    data = (
        make_df(
            "input",
            year_vtg=years.year_vtg,
            year_act=years.year_act,
            technology="t",
            mode="m",
            commodity=None,
            level="final",
            time="year",
            time_origin="year",
            value=0.05,
            unit="GWa / (Gv km)",
        )
        .pipe(broadcast, node_loc=info.N)
        .pipe(same_node)
    )

    # Generate random commodity values
    c = ("electr", "ethanol", "gas", "hydrogen", "lightoil", "methanol")
    splitter = np.random.choice(np.arange(len(c)), len(data))
    data = data.assign(
        commodity=pd.Categorical.from_codes(splitter, categories=c),
    )
    assert not data.isna().any().any(), data

    # Function runs successfully on these data
    result = ef_for_input(test_context, data)

    # Returns data for two parameters if transport.Config.emission_relations is True
    # (the default)
    assert {"emission_factor", "relation_activity"} == set(result)
    ef = result["emission_factor"]

    # Data is complete
    assert not ef.isna().any().any(), ef

    # Data have the expected columns
    assert sorted(make_df("emission_factor").columns) == sorted(ef.columns)

    ra = result["relation_activity"]
    assert not ra.isna().any(axis=None), ra

    assert ra.dtypes["year_act"] == int  # noqa: E721

    # print(ra.to_string())

    # TODO test specific values


@pytest.mark.parametrize("source, rows", (("1", 4717), ("2", 5153)))
@pytest.mark.parametrize("regions", ["R11"])
def test_get_emissions_data(test_context, source, rows, regions):
    # Set the value; don't need to read_config()
    test_context.model.regions = regions
    test_context.transport = Config(data_source=DataSourceConfig(emissions=source))

    data = get_emissions_data(test_context)
    assert {"emission_factor"} == set(data.keys())
    assert rows == len(data["emission_factor"])
