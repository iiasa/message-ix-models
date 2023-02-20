from message_data.model.transport import build


def configure_build(context, regions, years):
    context.update(regions=regions, years=years)

    build.get_computer(context)

    return context["transport build info"]
