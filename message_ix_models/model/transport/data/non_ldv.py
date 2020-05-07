from message_data.tools import get_context


def get_non_ldv_data(info):
    config = get_context()['transport config']['data source']
    source = config.get('non-LDV', None)

    if source == 'IKARUS':
        from . import ikarus
        return ikarus.get_ikarus_data(info)
    elif source is None:
        return {}  # Don't add any data
    else:
        raise ValueError(f'invalid source for non-LDV data: {source}')
