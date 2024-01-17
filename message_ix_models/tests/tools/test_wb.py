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


def test_assign_income_groups(nodes="R12") -> None:
    cl_node = get_codelist(f"node/{nodes}")
    cl_ig = get_income_group_codelist()
    assign_income_groups(cl_node, cl_ig)

    expected = {
        "R12_AFR": "Lower middle income",
        "R12_RCPA": "Lower middle income",
        "R12_CHN": "Upper middle income",
        "R12_EEU": "High income",
        "R12_FSU": "Upper middle income",
        "R12_LAM": "Upper middle income",
        "R12_MEA": "Lower middle income",
        "R12_NAM": "High income",
        "R12_PAO": "High income",
        "R12_PAS": "Lower middle income",
        "R12_SAS": "Lower middle income",
        "R12_WEU": "High income",
    }

    # Each node belongs to the expected income group
    for node, ig in expected.items():
        assert ig == str(cl_node[node].get_annotation(id="wb-income-group").text)
