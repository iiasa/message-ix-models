from message_data.model.transport.utils import read_config


def test_read_config(test_context):
    read_config(test_context)

    # Data table loaded
    assert 'mer_to_ppp' in test_context.data

    # Scalar parameters loaded
    assert 'scaling' in test_context.data
    assert test_context.data['whours'] == 200 * 8
