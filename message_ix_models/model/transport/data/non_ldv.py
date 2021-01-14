def get_non_ldv_data(context):
    source = context["transport config"]["data source"].get("non-LDV", None)

    if source == "IKARUS":
        from .ikarus import get_ikarus_data

        return get_ikarus_data(context)
    elif source is None:
        return {}  # Don't add any data
    else:
        raise ValueError(f"invalid source for non-LDV data: {source}")
