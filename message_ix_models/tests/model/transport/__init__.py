from message_ix_models.model import bare

from message_data.model.transport import Config, build


def configure_build(context, regions, years):
    context.update(regions=regions, years=years)

    Config.from_context(context)

    # Information about the corresponding base model
    info = bare.get_spec(context)["add"]
    context["transport build info"] = info
    context["transport spec"] = build.get_spec(context)

    return info
