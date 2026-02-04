from message_ix_models.project.circeular.structure import CL_TRANSPORT_SCENARIO


class TestCL_TRANSPORT_SCENARIO:
    def test_create(self) -> None:
        # Function runs without error
        result = CL_TRANSPORT_SCENARIO.create()

        # Code list has expected number of items
        assert 8 == len(result)

        # An expected item is in the code list
        item = result["CC-C-D-D"]
        # Description contains scenario information
        assert "regional=convergence, material=default" == str(item.description)
