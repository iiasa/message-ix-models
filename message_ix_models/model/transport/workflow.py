from message_ix_models.project.ssp import SSP_2024


def generate(context, **options):
    from message_ix_models import Workflow

    from message_data.projects import navigate
    from message_data.projects.navigate.workflow import report, solve

    from . import build
    from .config import Config

    # TODO respect dry-run
    # TODO respect quiet

    # Prepare transport configuration
    Config.from_context(context, options)

    # Set values expected by workflow steps re-used from .projects.navigate
    context.navigate = navigate.Config(
        scenario="baseline",
        # buildings=False,
    )

    wf = Workflow(context)

    # Load base scenario
    if not context.url:
        raise RuntimeError("Must provide a --url")
    wf.add_step("base", None, target=context.url)

    all_keys = []
    for ssp in SSP_2024:
        # Construct a label including the SSP
        label = f"SSP{ssp.name}"
        label_full = f"SSP_2024.{ssp.name}"

        # Identify the target of this step
        target = (
            f"{context.dest} {label_full}"
            if context.dest
            else (
                f"MESSAGEix-GLOBIOM 1.1-T-{context.model.regions}/{label_full} baseline"
            )
        )

        # Build Transport on the scenario
        # TODO Add functionality like gen-activity
        # TODO Add functionality like build_cmd() with report_build
        wf.add_step(
            f"{label} built", "base", build.main, target=target, clone=True, ssp=ssp
        )

        # Solve
        wf.add_step(f"{label} solved", f"{label} built", solve)

        # Report
        all_keys.append(wf.add_step(f"{label} reported", f"{label} solved", report))

    wf.add("all reported", all_keys)
    wf.default_key = "all reported"

    return wf
