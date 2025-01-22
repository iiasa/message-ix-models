from typing import TYPE_CHECKING, Optional

import pytest

from message_ix_models.model.structure import get_codelist
from message_ix_models.tools.wb import (
    assign_income_groups,
    get_income_group_codelist,
    make_map,
)

if TYPE_CHECKING:
    import sdmx.model.common

# TODO Remove once WB servers consistently provide the updated file; i.e. all of the
#      marked tests XPASS.
MARK = pytest.mark.xfail(
    raises=ValueError,
    reason="SHA256 hash of downloaded file .* does not match .* got but got "
    "9b8452db52e391602c9e9e4d4ef4d254f505ce210ce6464497cf3e40002a3545",
)


@pytest.fixture(scope="module")
def cl_ig() -> "sdmx.model.common.Codelist":
    return get_income_group_codelist()


@MARK
def test_get_income_group_codelist(cl_ig: "sdmx.model.common.Codelist") -> None:
    def n(id) -> int:
        return len(cl_ig[id].child)

    # Groups counts are as expected and have the expected relationships
    assert 25 == n("LIC")
    assert 108 == n("MIC") == n("LMC") + n("UMC")
    assert 77 == n("HIC")
    # NB +1 is for VEN, which is not categorized
    assert n("WLD") == n("LIC") + n("MIC") + n("HIC") + 1

    # Annotations exist and have expected values
    assert (
        "urn:sdmx:org.sdmx.infomodel.codelist.Code=WB:CL_REF_AREA_WDI(1.0).HIC"
        == str(cl_ig["ABW"].get_annotation(id="wb-income-group").text)
    )
    assert "IDA" == str(cl_ig["AFG"].get_annotation(id="wb-lending-category").text)


REPLACE = (
    {  # index 0
        "HIC": "HIC",
        "UMC": "LMIC",
        "LMC": "LMIC",
        "LIC": "LMIC",
    },
)

EXP = {
    ("R12", "count", None): {
        "R12_AFR": "LIC",
        "R12_RCPA": "LMC",
        "R12_CHN": "HIC",
        "R12_EEU": "HIC",
        "R12_FSU": "UMC",
        "R12_LAM": "UMC",
        "R12_MEA": "HIC",
        "R12_NAM": "HIC",
        "R12_PAO": "HIC",
        "R12_PAS": "LMC",
        "R12_SAS": "LMC",
        "R12_WEU": "HIC",
    },
    ("R12", "population", None): {
        "R12_AFR": "LMC",
        "R12_RCPA": "LMC",
        "R12_CHN": "UMC",
        "R12_EEU": "HIC",
        "R12_FSU": "UMC",
        "R12_LAM": "UMC",
        "R12_MEA": "LMC",
        "R12_NAM": "HIC",
        "R12_PAO": "HIC",
        "R12_PAS": "UMC",
        "R12_SAS": "LMC",
        "R12_WEU": "HIC",
    },
    ("R12", "population", 0): {
        "R12_AFR": "LMIC",
        "R12_RCPA": "LMIC",
        "R12_CHN": "LMIC",
        "R12_EEU": "HIC",
        "R12_FSU": "LMIC",
        "R12_LAM": "LMIC",
        "R12_MEA": "LMIC",
        "R12_NAM": "HIC",
        "R12_PAO": "HIC",
        "R12_PAS": "LMIC",
        "R12_SAS": "LMIC",
        "R12_WEU": "HIC",
    },
}


@MARK
@pytest.mark.parametrize(
    "nodes, method, replace",
    (
        ("R12", "population", None),
        ("R12", "count", None),
        ("R12", "population", 0),
    ),
)
def test_assign_income_groups(
    cl_ig: "sdmx.model.common.Codelist", nodes: str, method: str, replace: Optional[int]
) -> None:
    cl_node = get_codelist(f"node/{nodes}")

    # Function runs without error
    replace_arg = make_map(REPLACE[replace]) if replace is not None else None

    assign_income_groups(cl_node, cl_ig, method, replace=replace_arg)

    # Each node belongs to the expected income group
    for node, ig in EXP[nodes, method, replace].items():
        c_node = cl_node[node]
        assert str(c_node.get_annotation(id="wb-income-group").text).endswith(ig), (
            c_node,
            c_node.annotations,
        )


def test_make_map() -> None:
    # Function runs
    result = make_map(REPLACE[0])

    # Expected result
    assert {
        "urn:sdmx:org.sdmx.infomodel.codelist.Code=WB:CL_REF_AREA_WDI(1.0).HIC": "HIC",
        "urn:sdmx:org.sdmx.infomodel.codelist.Code=WB:CL_REF_AREA_WDI(1.0).LMC": "LMIC",
        "urn:sdmx:org.sdmx.infomodel.codelist.Code=WB:CL_REF_AREA_WDI(1.0).UMC": "LMIC",
        "urn:sdmx:org.sdmx.infomodel.codelist.Code=WB:CL_REF_AREA_WDI(1.0).LIC": "LMIC",
    } == result
