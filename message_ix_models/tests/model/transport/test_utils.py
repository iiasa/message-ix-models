import xarray as xr

from message_data.model.transport.utils import consumer_groups, read_config


def test_read_config(session_context):
    # read_config() returns a reference to the current context
    context = read_config()
    assert context is session_context

    # Data table is loaded
    assert 'mer_to_ppp' in context.data

    # Scalar parameters are loaded
    assert "scaling" in context["transport config"]
    assert context["transport config"]["work hours"] == 200 * 8


def test_consumer_groups(session_context):
    # Returns a list of codes
    codes = consumer_groups()
    RUEAA = codes[codes.index('RUEAA')]
    assert RUEAA.name == 'Rural, or “Outside MSA”, Early Adopter, Average'

    # Returns xarray objects for indexing
    result = consumer_groups(rtype='indexers')
    assert all(isinstance(da, xr.DataArray) for da in result.values())
