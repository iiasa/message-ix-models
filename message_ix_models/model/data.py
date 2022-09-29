import logging

log = logging.getLogger(__name__)


def get_data(scenario, context, spec, **options):
    """Data for the bare RES."""
    if context.model.res_with_dummies:
        log.warning("get_dummy_data() not migrated")
        # return get_dummy_data(scenario, spec)
    else:
        return dict()
