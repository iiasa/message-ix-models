"""Reporting for the MESSAGEix-GLOBIOM global model."""
from pathlib import Path
import logging

from .core import CONFIG, prepare_reporter, register

__all__ = [
    "CONFIG",
    "prepare_reporter",
    "register",
    "report",
]


log = logging.getLogger(__name__)


def report(
    scenario,
    key=None,
    config=None,
    output_path=None,
    dry_run=False,
    **kwargs
):
    """Run complete reporting on *scenario* with output to *output_path*.

    This function provides a common interface to call both the 'new'
    (:mod:`.reporting`) and 'legacy' (:mod:`.tools.post_processing`) reporting
    codes.

    Parameters
    ----------
    scenario : Scenario
        Solved Scenario to be reported.
    key : str or Key
        Key of the report or quantity to be computed. Default: ``'default'``.
    config : Path-like, optional
        Path to reporting configuration file. Default: :file:`global.yaml`.
    output_path : Path-like
        Path to reporting
    dry_run : bool, optional
        Only show what would be done.

    Other parameters
    ----------------
    path : Path-like
        Deprecated alias for `output_path`.
    legacy : dict
        If given, the old-style reporting in
        :mod:`.tools.post_processing.iamc_report_hackathon` is used, and
        `legacy` is used as keyword arguments.
    """

    if "path" in kwargs:
        log.warning("Deprecated: path= kwarg to report(); use output_path=")
        if output_path:
            raise RuntimeError(
                f"Ambiguous: output_path={output_path}, path={kwargs['path']}"
            )
        output_path = kwargs.pop("path")

    if "legacy" in kwargs:
        log.info("Using legacy tools.post_processing.iamc_report_hackathon")
        from message_data.tools.post_processing import iamc_report_hackathon

        legacy_args = dict(merge_hist=True)
        legacy_args.update(**kwargs["legacy"])

        return iamc_report_hackathon.report(
            mp=scenario.platform,
            scen=scenario,
            model=scenario.model,
            scenario=scenario.scenario,
            out_dir=output_path,
            **legacy_args,
        )

    # Default arguments
    key = key or "default"
    config = config or (
        Path(__file__).parents[2] / "data" / "report" / "global.yaml"
    )

    rep, key = prepare_reporter(scenario, config, key, output_path)

    log.info(f"Prepare to report:\n\n{rep.describe(key)}")

    if dry_run:
        return

    result = rep.get(key)

    msg = f" written to {output_path}" if output_path else f":\n{result}"
    log.info(f"Result{msg}")
