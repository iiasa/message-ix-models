from message_ix_models.model.transport.files import collect_structures, read_structures


def test_collect_structures():
    sm1 = collect_structures()

    sm2 = read_structures()

    # Structures are retrieved from file successfully
    # The value is either 30 or 31 depending on whether .build.add_exogenous_data() has
    # run
    assert 30 <= len(sm1.dataflow) == len(sm2.dataflow)
