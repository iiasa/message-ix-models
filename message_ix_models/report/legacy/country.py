"""Legacy reporting of single-country models.

See :ref:`report-legacy-country`.
"""

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from message_ix import Scenario

log = logging.getLogger(__name__)

#: Prefix of the names of statistical-difference technologies. Each is active in the
#: calibration year only, and either supplies a commodity at a level (mode M1, output),
#: takes it up (mode M2, input), or enters a relation (:par:`relation_activity`).
STAT_DIFF = "stat_diff_"

#: Conversion factor from GWa to EJ.
GWa_to_EJ = 8.76 * 3.6e-3

#: Ratio of the masses of CO₂ and C.
CO2_C = 44 / 12

#: Totals compared with the sum of their parts in :func:`closure`. The legacy total of
#: Primary Energy leaves out Primary Energy|Secondary Energy Trade.
PARTS = {
    "Final Energy": (
        "sectors",
        [
            "Industry",
            "Non-Energy Use",
            "Residential and Commercial",
            "Transportation",
            "Other Sector",
        ],
    ),
    "Final Energy ": (
        "fuels",
        [
            "Electricity",
            "Gases",
            "Heat",
            "Hydrogen",
            "Liquids",
            "Solar",
            "Solids",
            "Geothermal",
        ],
    ),
    "Primary Energy": (
        "fuels",
        [
            "Biomass",
            "Coal",
            "Gas",
            "Oil",
            "Nuclear",
            "Hydro",
            "Solar",
            "Wind",
            "Geothermal",
            "Ocean",
            "Other",
        ],
    ),
}


@contextmanager
def _transact_ts(scenario: "Scenario", message: str) -> Iterator[None]:
    """Check out time series data only, and commit; discard changes on error."""
    scenario.check_out(timeseries_only=True)
    try:
        yield
    except BaseException:
        scenario.discard_changes()
        raise
    scenario.commit(message)


def register_regions(scenario: "Scenario") -> list[str]:
    """Register the nodes of `scenario` that its platform does not know as regions.

    :meth:`ixmp.TimeSeries.add_timeseries` refuses data for unregistered regions.
    Returns the names registered.
    """
    mp = scenario.platform
    known = set(mp.regions()["region"])
    added = [n for n in scenario.set("node") if n not in known]
    for n in added:
        log.info(f"Register region {n!r} on platform {mp.name!r}")
        mp.add_region(n, "region", "World")
    return added


def clear_timeseries(scenario: "Scenario") -> None:
    """Remove time series data of `scenario` from the first model year on."""
    ts = scenario.timeseries()
    ts = ts.loc[ts.year >= scenario.firstmodelyear]
    if len(ts):
        log.info(f"Remove {len(ts)} reported time series values")
        with _transact_ts(scenario, "Clear reported time series"):
            scenario.remove_timeseries(ts)


def report_materials(scenario: "Scenario", out_dir: Path | None, **kwargs) -> None:
    """Run the materials reporting and store its results as time series data.

    `kwargs` are passed to :func:`.material.report.run_reporting.run`. If `out_dir` is
    given, the results are also written to :file:`materials.csv` there.
    """
    from message_ix_models.model.material.report.run_reporting import run

    with _transact_ts(scenario, "Add materials reporting"):
        df = run(scenario, upload_ts=True, **kwargs)
    log.info(f"Materials reporting: {len(df.data)} values")
    if out_dir:
        df.data.to_csv(out_dir.joinpath("materials.csv"), index=False)


def stat_diff_data(
    act: pd.DataFrame,
    input: pd.DataFrame,
    output: pd.DataFrame,
    relation_activity: pd.DataFrame,
    model: str,
    scenario: str,
    region: str | None = None,
) -> pd.DataFrame:
    """Return IAMC-structured data for the statistical differences of a model.

    Parameters
    ----------
    act
        Variable :math:`ACT` of the statistical-difference technologies, with columns
        "node_loc", "technology", "year_act", "mode" and "lvl".
    input, output, relation_activity
        The respective parameter data of the same technologies.
    region
        If given, the values are also summed into this region.

    Returns
    -------
    pandas.DataFrame
        with the columns "model", "scenario", "region", "variable", "unit", "year" and
        "value". A commodity supplied or taken up appears as
        :py:`"Statistical Difference|{commodity}|{level}"` in EJ/yr; a supply is
        positive, a use negative. A relation appears as
        :py:`"Statistical Difference|{relation}"` in GWa: the activity of the technology
        times its coefficient in the relation.
    """
    cols = ["model", "scenario", "region", "variable", "unit", "year", "value"]
    if act.empty:
        return pd.DataFrame(columns=cols)

    idx = ["node_loc", "technology", "year_act", "mode"]
    act = act.groupby(idx, as_index=False)["lvl"].sum()

    dfs = []
    for par, sign in ((output, 1.0), (input, -1.0)):
        if par.empty:
            continue
        df = act.merge(par[idx + ["commodity", "level", "value"]], on=idx)
        dfs.append(
            df.assign(
                variable="Statistical Difference|" + df.commodity + "|" + df.level,
                unit="EJ/yr",
                value=sign * df.lvl * df.value * GWa_to_EJ,
            )
        )
    if not relation_activity.empty:
        df = act.merge(relation_activity[idx + ["relation", "value"]], on=idx)
        dfs.append(
            df.assign(
                variable="Statistical Difference|" + df.relation,
                unit="GWa",
                value=df.lvl * df.value,
            )
        )

    if not dfs:
        return pd.DataFrame(columns=cols)

    result = (
        pd.concat(dfs)
        .rename(columns={"node_loc": "region", "year_act": "year"})
        .groupby(["region", "variable", "unit", "year"], as_index=False)["value"]
        .sum()
    )
    if region:
        total = result.groupby(["variable", "unit", "year"], as_index=False)["value"]
        result = pd.concat([result, total.sum().assign(region=region)])
    return result.assign(model=model, scenario=scenario)[cols].reset_index(drop=True)


def add_stat_diff(scenario: "Scenario", region: str | None = None) -> pd.DataFrame:
    """Store the statistical differences of `scenario` as time series data.

    See :func:`stat_diff_data`. Returns the data stored, possibly empty.
    """
    t = [t for t in scenario.set("technology") if t.startswith(STAT_DIFF)]
    if not t:
        log.info("No statistical-difference technologies")
        e = pd.DataFrame()
        return stat_diff_data(e, e, e, e, scenario.model, scenario.scenario)

    filters = dict(technology=t)
    data = stat_diff_data(
        scenario.var("ACT", filters=filters),
        scenario.par("input", filters=filters),
        scenario.par("output", filters=filters),
        scenario.par("relation_activity", filters=filters),
        scenario.model,
        scenario.scenario,
        region,
    )
    nonzero = data[data.value.abs() > 0]
    log.info(
        f"{len(t)} statistical-difference technologies; nonzero:\n"
        + nonzero.to_string(index=False)
    )
    with _transact_ts(scenario, "Add statistical differences"):
        scenario.add_timeseries(data)
    return data


def closure(
    ts: pd.DataFrame, act: pd.DataFrame, emiss: pd.DataFrame, rtol: float = 1e-3
) -> pd.DataFrame:
    """Compare reported totals with the solution and with the sum of their parts.

    Parameters
    ----------
    ts
        Reported time series data of one region, with columns "variable", "year" and
        "value".
    act
        Variable :math:`ACT` of technology CO2_TCE in the same region.
    emiss
        Variable :math:`EMISS` of emission TCE, type_tec "all", in the same region.

    Returns
    -------
    pandas.DataFrame
        with one row per check and year: "check", "reference" (description of the
        reference), "year", "reported", "expected" and "ok" (:any:`True` if the two
        agree within `rtol`).
    """
    ts = ts.pivot_table(index="variable", columns="year", values="value")
    years = list(ts.columns)

    def row(v: str) -> pd.Series:
        return ts.loc[v] if v in ts.index else pd.Series(float("nan"), index=years)

    def sol(df: pd.DataFrame, dim: str) -> pd.Series:
        return df.groupby(dim)["lvl"].sum().reindex(years, fill_value=0.0) * CO2_C

    checks = [
        ("Emissions|CO2", "ACT of CO2_TCE x 44/12", sol(act, "year_act")),
        ("Emissions|Kyoto Gases", "EMISS of TCE x 44/12", sol(emiss, "year")),
    ]
    for name, (what, parts) in PARTS.items():
        total = name.strip()
        have = [f"{total}|{p}" for p in parts if f"{total}|{p}" in ts.index]
        checks.append((total, f"sum of {len(have)} {what}", ts.loc[have].sum()))

    rows = []
    for name, ref, expected in checks:
        reported = row(name)
        for y in years:
            r, e = reported[y], expected[y]
            ok = abs(r - e) <= rtol * max(abs(e), 1e-9)
            rows.append((name, ref, y, r, e, ok))
    return pd.DataFrame(
        rows, columns=["check", "reference", "year", "reported", "expected", "ok"]
    )


def log_closure(scenario: "Scenario") -> pd.DataFrame:
    """Log :func:`closure` for every node of `scenario` that is not a global region."""
    first = scenario.firstmodelyear
    result = []
    for n in scenario.set("node"):
        if n == "World" or n.endswith("_GLB"):
            continue
        ts = scenario.timeseries(region=n)
        ts = ts.loc[ts.year >= first]
        act = scenario.var("ACT", filters=dict(node_loc=[n], technology=["CO2_TCE"]))
        emiss = scenario.var(
            "EMISS", filters=dict(node=[n], emission=["TCE"], type_tec=["all"])
        )
        df = closure(ts, act, emiss).assign(node=n)
        for (check, ref), group in df.groupby(["check", "reference"], sort=False):
            status = "OK " if group.ok.all() else "GAP"
            cells = "  ".join(
                f"{r.year}: {r.reported:.4g} / {r.expected:.4g}"
                for r in group.itertuples()
                if not r.ok or r.year in (first, 2030, 2050)
            )
            log.info(f"{status} {n} {check} vs {ref}: {cells}")
        result.append(df)
    return pd.concat(result) if result else pd.DataFrame()
