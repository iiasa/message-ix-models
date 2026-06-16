from message_ix_models.project.digsy.structure import CL_SCENARIO_DIGSY, SCENARIO

URN = (
    "urn:sdmx:org.sdmx.infomodel.codelist.Code="
    "IIASA_ECE:CL_SCENARIO_DIGSY(0.2.0).WORST-C"
)


class TestCL_SCENARIO_DIGSY:
    def test_create(self) -> None:
        # Code list can be generated
        result = CL_SCENARIO_DIGSY.get(force=True)

        # Code list has expected number of members
        assert 6 == len(result)

        # Items have URNs that can be referenced
        assert URN == result["WORST-C"].urn


class TestSCENARIO:
    def test_members(self) -> None:
        # Members can be accessed by name
        SCENARIO["BASE"]
        SCENARIO["BEST-C"]
        SCENARIO["WORST-C"]
        SCENARIO["_Z"]

        # Members can be accessed as attributes with aliased, valid Python names
        assert SCENARIO["BASE"] == SCENARIO.BASE  # type: ignore [attr-defined]
        assert SCENARIO["BEST-C"] == SCENARIO.BEST_C  # type: ignore [attr-defined]
        assert SCENARIO["WORST-C"] == SCENARIO.WORST_C  # type: ignore [attr-defined]
        assert SCENARIO["_Z"] == SCENARIO._Z  # type: ignore [attr-defined]

        # Members can be accessed by URN
        assert SCENARIO["WORST-C"] is SCENARIO.by_urn(URN)

    def test_urn(self) -> None:
        # Member URN can be retrieved
        assert URN == SCENARIO["WORST-C"].urn
