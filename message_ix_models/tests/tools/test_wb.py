import pytest

from message_ix_models.model.structure import get_codelist
from message_ix_models.tools.wb import assign_income_groups, get_income_group_codelist


def test_get_income_group_codelist() -> None:
    cl = get_income_group_codelist()

    def n(id) -> int:
        return len(cl[id].child)

    # Groups counts are as expected and have the expected relationships
    assert 25 == n("LIC")
    assert 108 == n("MIC") == n("LMC") + n("UMC")
    assert 77 == n("HIC")
    # NB +1 is for VEN, which is not categorized
    assert n("WLD") == n("LIC") + n("MIC") + n("HIC") + 1

    # Annotations exist and have expected values
    assert (
        "urn:sdmx:org.sdmx.infomodel.codelist.Code=WB:CL_REF_AREA_WDI(1.0).HIC"
        == str(cl["ABW"].get_annotation(id="wb-income-group").text)
    )
    assert "IDA" == str(cl["AFG"].get_annotation(id="wb-lending-category").text)


EXP = {
    ("R12", "count"): {
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
    ("R12", "population"): {
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
}


@pytest.mark.parametrize(
    "nodes, method",
    (
        ("R12", "population"),
        ("R12", "count"),
    ),
)
def test_assign_income_groups(nodes: str, method: str) -> None:
    cl_node = get_codelist(f"node/{nodes}")
    cl_ig = get_income_group_codelist()

    # Function runs without error
    assign_income_groups(cl_node, cl_ig, method)

    # Each node belongs to the expected income group
    for node, ig in EXP[nodes, method].items():
        assert str(cl_node[node].get_annotation(id="wb-income-group").text).endswith(
            ig
        ), node
