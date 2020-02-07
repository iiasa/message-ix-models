import xarray as xr

from message_data.model.transport.utils import consumer_groups, read_config


def test_read_config(test_context):
    read_config(test_context)

    # Data table is loaded
    assert 'mer_to_ppp' in test_context.data

    # Scalar parameters are loaded
    assert 'scaling' in test_context.data
    assert test_context.data['whours'] == 200 * 8


def test_consumer_groups(test_context):
    # Returns a list of codes
    result = consumer_groups(test_context, rtype='code')
    assert 'RUEAA' in result

    # Returns tuples of (code, description)
    result = consumer_groups(test_context, rtype='description')
    assert ('RUEAA', 'rural, or “outside msa”, early adopter, average') \
        in result

    # Returns xarray objects for indexing
    result = consumer_groups(test_context, rtype='indexers')
    assert all(isinstance(da, xr.DataArray) for da in result.values())
