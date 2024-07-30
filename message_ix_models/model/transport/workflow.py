import json
import logging
from hashlib import blake2s
from itertools import product
from typing import TYPE_CHECKING, Literal, Optional

from genno import KeyExistsError

from message_ix_models.project.ssp import SSP_2024
from message_ix_models.util import package_data_path

if TYPE_CHECKING:
    import message_ix_models

    from .config import Config

log = logging.getLogger(__name__)


def base_scenario_url(
    context: "message_ix_models.Context", method: Literal["auto", "bare"] = "bare"
) -> str:
    """Identify the base MESSAGEix-GLOBIOM scenario.

    If :attr:`.scenario_info` is set on `context` (for instance, provided via the
    :program:`--url` CLI option), nothing is done, and the URL corresponding to that
    scenario is returned.

    If not, then the behaviour depends on `method`:

    :py:`method = "auto"`
       Automatically identify the base scenario URL from the contents of
       :file:`base-scenario-url.json`. The settings :attr:`.Config.ssp
       <.transport.config.Config.ssp>` and :attr:`.Config.policy` are used to match an
       entry in the file.
    :py:`method = "bare"`
       Construct bare RES scenario using :mod:`.model.bare.create_res` and the settings
       on `context` such as :attr:`.Config.regions`. Return the URL to this scenario.
    """
    if context.scenario_info:
        return context.core.url

    config: "Config" = context.transport

    if method == "auto":
        # Load URL info from file
        with open(package_data_path("transport", "base-scenario-url.json")) as f:
            info = json.load(f)

        # Identify a key that matches the settings on `config`
        key = (str(config.ssp), config.policy)
        for item in info:
            if (item["ssp"], item["policy"]) == key:
                return item["url"]

        raise ValueError(f"No base URL for ({key!r})")  # pragma: no cover
    elif method == "bare":
        # Use a 'bare' RES or empty scenario
        if context.platform_info["name"] in (__name__, "message-ix-models"):
            # Temporary platform or testing → use the bare RES
            from message_ix_models.model import bare

            log.info("No --model/--scenario/--url; use the bare RES as base")
            # Build a bare RES scenario given .model.Config settings
            s = bare.create_res(context)

            return f"ixmp://{context.platform_info['name']}/{s.url}"
        else:
            log.warning("No --model/--scenario/--url; some workflow steps may not work")
            return f"ixmp://{context.platform_info.get('name', 'NONE')}/NONE/NONE"


def maybe_use_temporary_platform(context: "message_ix_models.Context") -> None:
    """Set up a temporary, in-memory platform.

    .. todo:: Move upstream, to :mod:`message_ix_models`.
    """
    if context.platform_info:
        return

    from ixmp import config as ixmp_config

    ixmp_config.add_platform(
        __name__, "jdbc", "hsqldb", url=f"jdbc:hsqldb:mem:{__name__}"
    )
    context.platform_info.update(name=__name__)
    log.info("No --platform/--url; using temporary, in-memory database")


def scenario_url(
    context: "message_ix_models.Context", label: Optional[str] = None
) -> str:
    """Construct a target URL for a built MESSAGEix-Transport scenario.

    If the :attr:`.dest` URL is set on `context` (for instance, provided via the
    :program:`--dest` CLI option), this URL returned with `label` appended to the
    scenario name.

    If not, a form is used like:

    - :py:`model = "MESSAGEix-GLOBIOM 1.1-T-{regions}"`. Any value of the "model" key
      from :attr:`.core.Config.dest_scenario` is appended.
    - :py:`scenario = "{label}"`. Any value of the "scenario" key from
      :attr:`.core.Config.dest_scenario` is appended; if this is not set, then either
      "policy" (if :attr:`.transport.Config.policy` is set) or "baseline".
    """
    # Construct a URL template for MESSAGEix-Transport scenarios
    if context.core.dest:
        # Value from --dest CLI option
        # TODO Check that this works if a version # is specified
        return f"{context.dest} {label or ''}".strip()
    else:
        # Values from --model-extra, --scenario-extra CLI options
        m_extra = context.core.dest_scenario.get("model", "")
        s_extra = context.core.dest_scenario.get("scenario") or (
            "policy" if context.transport.policy else "baseline"
        )

        return "/".join(
            (
                f"MESSAGEix-GLOBIOM 1.1-T-{context.model.regions} {m_extra}".rstrip(),
                f"{label or ''} {s_extra}".strip(),
            )
        )


def short_hash(value: str) -> str:
    """Return a short (length 3) hash of `value`."""
    return blake2s(value.encode()).hexdigest()[:3]


def generate(
    context: "message_ix_models.Context",
    *,
    report_key="transport all",
    dry_run: bool = False,
    **options,
):
    from message_ix_models import Workflow
    from message_ix_models.model.workflow import Config as SolveConfig
    from message_ix_models.model.workflow import solve
    from message_ix_models.project import navigate
    from message_ix_models.report import register, report

    from . import build
    from .config import Config
    from .report import multi

    # Handle CLI options
    # TODO respect quiet
    options.pop("target_model_name")  # Stored on context.core.dest_scenario
    options.pop("target_scenario_name")  # Stored on context.core.dest_scenario
    base_scenario_method = options.pop("base_scenario")

    maybe_use_temporary_platform(context)

    # Prepare transport configuration, passing the remaining `options`
    Config.from_context(context, options=options)

    # Set values expected by workflow steps re-used from .projects.navigate
    context.navigate = navigate.Config(
        scenario="baseline", buildings=False, material=False
    )
    # Use lpmethod=4, scaind=1 to overcome LP status 5 (optimal with unscaled
    # infeasibilities) when running on SSP(2024) base scenarios
    solve_config = SolveConfig(
        reserve_margin=False,
        solve=dict(model="MESSAGE", solve_options=dict(lpmethod=4, scaind=1)),
    )
    # Set the default .report.Config key for ".* reported" steps
    register("model.transport")
    context.report.key = report_key

    # Create the workflow
    wf = Workflow(context)

    # Collections of step names
    debug, reported, targets = [], [], []

    # Iterate over all (ssp, policy) combinations
    for ssp, policy in product(SSP_2024, (False, True)):
        # Store settings on the context
        context.transport.ssp = ssp
        context.transport.policy = policy

        # Construct labels including the SSP code and policy identifier
        label = f"SSP{ssp.name}{' policy' if policy else ''}"  # For step name
        label_full = f"SSP_2024.{ssp.name}"  # For the scenario name

        # Identify the base scenario
        base_url = base_scenario_url(context, base_scenario_method)
        log.info(f"{label_full} {policy = }: {base_url = }")

        # Name of the base step
        base = f"base {short_hash(base_url)}"

        try:
            # Load the base model scenario
            wf.add_step(base, None, target=base_url)
        except KeyExistsError:
            # Base scenario URL is identical to another (ssp, policy) combination; use
            # that step
            pass

        # Identify the target of the build step
        target_url = scenario_url(context, label_full)
        targets.append(target_url)

        # Build MESSAGEix-Transport on the scenario
        wf.add_step(
            f"{label} built", base, build.main, target=target_url, clone=True, ssp=ssp
        )

        # 'Simulate' build and produce debug outputs
        debug.append(f"{label} debug build")
        wf.add_step(debug[-1], base, build.main, ssp=ssp, dry_run=True)

        # Solve
        wf.add_step(f"{label} solved", f"{label} built", solve, config=solve_config)

        # Report
        reported.append(f"{label} reported")
        wf.add_step(reported[-1], f"{label} solved", report)

    # NB the following use genno.Computer.add(), not .Workflow.add_step(). This is
    #    because the operations are not WorkflowSteps that receive, modify, and return
    #    Scenario objects—only ordinary Python functions.

    # Compare debug outputs from multiple simulated builds
    wf.add("debug build", build.debug_multi, "context", *debug)

    # Report (including plot) using data from multiple, solved scenarios
    wf.add("report multi", multi, "context", targets=targets)

    # Report all the scenarios
    wf.add("all reported", reported)
    wf.default_key = "all reported"

    return wf
