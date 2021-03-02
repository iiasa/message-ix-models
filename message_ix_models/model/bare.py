def create_res(context):
    # Temporary, only to enable testing of .util.click
    from message_ix.testing import make_dantzig

    return make_dantzig(context.get_platform())
