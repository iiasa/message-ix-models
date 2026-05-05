from message_ix_models.project.circeular.structure import (
    CL_SCENARIO,
    CL_SCENARIO_TRANSPORT,
)


class TestCL_SCENARIO:
    def test_create(self) -> None:
        # Function runs without error
        result = CL_SCENARIO.get()

        # Code list has expected number of items
        assert 6 == len(result)

        # An expected item is in the code list
        item = result["N"]
        # A short fragment is in the item URN
        assert "Code=IIASA_ECE:CL_SCENARIO_CIRCEULAR(1.0.0).N" in item.urn


class TestCL_SCENARIO_TRANSPORT:
    def test_create(self) -> None:
        # Function runs without error
        result = CL_SCENARIO_TRANSPORT.get()

        # Code list has expected number of items
        assert 8 == len(result)

        # An expected item is in the code list
        item = result["CC-C-D-D"]
        # Description contains scenario information
        assert "regional=convergence, material=default" == str(item.description)
