from message_ix_models.tools.wb import get_income_group_codelist


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
