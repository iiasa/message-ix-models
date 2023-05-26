import pooch


def fetch(args):
    p = pooch.create(path=pooch.os_cache("message-ix-models"), **args)
    assert 1 == len(p.registry)
    return p.fetch(next(iter(p.registry.keys())), progressbar=True)
