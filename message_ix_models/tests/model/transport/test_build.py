from message_data.model.transport.build import main as build


def test_build_bare_res(bare_res):
    """Test that model.transport.build works on the MESSAGEix-GLOBIOM RES."""
    build(bare_res)
