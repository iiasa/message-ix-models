from itertools import product

import pandas as pd
import pytest
from message_ix_models.model.structure import get_codes
from message_ix_models.util import nodes_ex_world

from message_data.tools.post_processing import pp_utils
from message_data.tools.post_processing.pp_utils import fil

#: These format strings appear in .default_tables.retr_hfc() in calls to :func:`.fil`.
FACTOR_EXPR = "refAC foam Fire ANMDI mVac Solv".split()

#: These values appear in default_run_config.yaml. The commented lines are those for
#: which :func:`.fil` is *not* called.
FACTOR_VAL_UNIT = (
    # (0, "kt HFC134a-equiv/yr"),  # Total  # Not called because hfc: "Total"
    (125, "kt HFC125/yr"),  # HFC125
    (134, "kt HFC134a/yr"),  # HFC134a
    (143, "kt HFC143a/yr"),  # HFC143a
    (227, "kt HFC227ea/yr"),  # HFC227ea
    # (0, "kt HFC23/yr"),  # HFC23  # Not called because run: "empty"
    (245, "kt HFC245fa/yr"),  # HFC245fa
    (32, "kt HFC32/yr"),  # HFC32
    (431, "kt HFC43-10/yr"),  # HFC430
    (365, "kt HFC365mfc/yr"),  # HFC365mfc  # Not called because run: False
    (152, "kt HFC152a/yr"),  # HFC152a
    (236, "kt HFC236fa/yr"),  # HFC236fa  # Not called because run: False
)


#: Input data expected by :func:`.fil`.
#:
#: - Axis named "Region" with short labels, e.g. "ABC" rather than "R##_ABC"
#: - Some columns with period labels, as integers.
#: - Float or NaN values.
FIL_DF = pd.DataFrame(
    [[1.0, None, 1.0]], columns=[2000, 2050, 2100], index=["AFR"]
).rename_axis("Region")


def pp_utils_globals(monkeypatch, regions):
    """Set :mod:`.pp_utils` global module variables.

    This utility function uses pytest's `monkeypatch` fixture to set the variables; the
    pre-existing values are restored after the end of each test.

    This only sets the globals that are used by :func:`.fil()`.
    """
    # region_id: the ID of the node code list; i.e. Context.regions
    monkeypatch.setattr(pp_utils, "region_id", regions)

    # regions: a mapping from model's native "R##_ABC" to "ABC", *plus* e.g. "R##_GLB"
    # mapped to # "World". This normally set by .iamc_report_hackathon.report().
    nodes = get_codes(f"node/{regions}")
    nodes = nodes[nodes.index("World")].child
    regions_map = {n.id: n.id.split("_")[1] for n in nodes_ex_world(nodes)}
    regions_map[f"{regions}_GLB"] = "World"
    monkeypatch.setattr(pp_utils, "regions", regions_map)

    # unit_conversion: a mapping of mappings. fil() directly sets "???" on its data and
    # then looks up mappings from "???" to `units_out`
    monkeypatch.setattr(
        pp_utils,
        "unit_conversion",
        {"???": {units: 1.0 for _, units in FACTOR_VAL_UNIT}},
    )

    # Expected index of data frame returned by fil()
    return set(regions_map.values())


@pytest.mark.parametrize("regions", ["R11", "R12"])
@pytest.mark.parametrize(
    "fil_arg, min_filled_values",
    [
        ("HFC_fac", 1),  # The only supported value
        ("foo", 0),  # Other values are essentially a no-op
    ],
)
def test_fil(monkeypatch, regions, fil_arg, min_filled_values):
    """Test :func:`.pp_utils.fil`.

    The test is parametrized for `regions` and each argument to :func:`.fil`.

    `min_filled_values` checks that at least N values in the resulting data frame are
    populated from the file(s).
    """
    # Monkey-patch the global variables in the pp_utils module used by fil() and
    # retrieve the expected index of the result
    expected_regions = pp_utils_globals(monkeypatch, regions)

    # Iterate over all values for the `factor` and `units` arguments that fil() is
    # expected to support
    for factor_expr, (factor_val, units) in product(FACTOR_EXPR, FACTOR_VAL_UNIT):
        # Mirror how values for the `factor` argument are constructed in
        # .default_tables.retr_hfc(), e.g. "Fire134"
        factor = f"{factor_expr}{factor_val}"

        # Function executes without error
        result = fil(FIL_DF, fil_arg, factor, units)

        # Returned data are a pd.DataFrame with the expected index
        assert expected_regions == set(result.index)

        # Number of NaN values in results is as expected
        assert min_filled_values <= (
            result.notnull().sum().sum() - FIL_DF.notnull().sum().sum()
        )
