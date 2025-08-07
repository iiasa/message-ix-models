from message_ix_models.project.digsy.structure import SCENARIO, get_cl_scenario

URN = "urn:sdmx:org.sdmx.infomodel.codelist.Code=IIASA_ECE:DIGSY_SCENARIO(0.1).WORST"


class TestSCENARIO:
    def test_members(self) -> None:
        # Members can be accessed as attributes
        SCENARIO.BEST  # type: ignore [attr-defined]
        SCENARIO.WORST  # type: ignore [attr-defined]
        SCENARIO.BASE  # type: ignore [attr-defined]
        SCENARIO._Z  # type: ignore [attr-defined]

        # Members can be accessed by name
        SCENARIO["BASE"]
        SCENARIO["BEST"]
        SCENARIO["WORST"]
        SCENARIO["_Z"]

        # Members can be accessed by URN
        assert SCENARIO["WORST"] is SCENARIO.by_urn(URN)

    def test_urn(self) -> None:
        # Member URN can be retrieved
        assert URN == SCENARIO["WORST"].urn


def test_get_cl_scenario() -> None:
    # Code list can be generated
    result = get_cl_scenario()

    # Code list has expected number of members
    assert 4 == len(result)

    # Items have URNs that can be referenced
    assert URN == result["WORST"].urn
