"""Retrieve IEA country-within-region energy weights.

Country weight inside a MESSAGE region::

    weight_iso = E(iso, year) / E(region, year)

``E`` is total primary energy (``Pe``), electricity generation (``Eg``), or
final energy (``Fe``). Only ``Pe`` is implemented; ``Eg`` / ``Fe`` are stubs.

Requires IIASA network access to the IEA Oracle DB, ``cx_Oracle``/``oracledb``,
and :mod:`message_data` (SQL helpers + :file:`data/model/IEA/config.yaml`).
ISO → region mapping uses
:func:`~message_ix_models.model.structure.get_codelist`.

Example
-------
::

    from message_ix_models.tools.iea.iso_share import retrieve_energy_shares

    df = retrieve_energy_shares("Pe", year=2015)
    df.to_csv("iso_share_pe.csv", index=False)
"""

from __future__ import annotations

import logging

import pandas as pd
import yaml
from yaml.loader import SafeLoader

from message_ix_models.model.structure import get_codelist
from message_ix_models.util import private_data_path

log = logging.getLogger(__name__)

#: IEA Oracle connection metadata under :mod:`message_data`.
IEA_CONFIG = private_data_path("model", "IEA", "config.yaml")

# Defaults mirror AddPolicies.__init__
DEFAULT_BIOMASS = "rhs"
DEFAULT_PE_ACCOUNTING = "direct"
DEFAULT_SUBST_ISO = {"CHN", "VNM", "LAO", "KHM", "PRK", "MNG"}
DEFAULT_BIOMASS_NC_REG = ("CHN", "RCPA", "SAS", "PAS", "LAM", "AFR", "MEA")
DEFAULT_SUBST_REG = ("R12_CHN",)


def load_iso_mapping(region_id: str = "R12") -> pd.DataFrame:
    """Return ISO 3166-1 alpha-3 → MESSAGE region mapping from the node codelist."""
    cl = get_codelist(f"node/{region_id}")
    rows = [
        {
            "iso": country.id,
            "country": str(country.name) if country.name else country.id,
            "region": region.id,
        }
        for region in cl["World"].child
        for country in region.child
    ]
    return pd.DataFrame(rows).set_index("iso")


# To be updated to IEA2024 via message-static-data
def connect_iea(region_id: str = "R12"):
    """Connect to the IEA WEB Oracle DB using private :file:`config.yaml`."""
    try:
        import cx_Oracle as oracle
    except ImportError:  # pragma: no cover
        import oracledb as oracle  # type: ignore

    from message_data.tools.utilities.iea_web_queries import IEAWebQueries

    with open(IEA_CONFIG) as f:
        meta = yaml.load(f, Loader=SafeLoader)[region_id]
    sql = IEAWebQueries(
        rev_code=meta["IEA_revision"],
        p_scheme=meta["IEA_scheme"],
        r_scheme=meta["IEA_region_scheme"],
        edb_data=meta["IEA_table"],
        biomass_nc_reg=DEFAULT_BIOMASS_NC_REG,
    )
    return oracle.connect(meta["IEA_link"]), sql, meta


def run_sql(curs, sql: str) -> float:
    """Execute `sql` and return the third column of the first row (or 0)."""
    curs.execute(sql)
    rows = curs.fetchall()
    return float(rows[0][2]) if rows else 0.0


def iso_total_pe_sql(
    sql,
    iso: str,
    year: int,
    reg_short: str,
    *,
    pe_accounting: str = DEFAULT_PE_ACCOUNTING,
    biomass: str = DEFAULT_BIOMASS,
) -> str:
    """Match ``AddPolicies._retrieve_iso_total_primary_energy``."""
    if (
        pe_accounting == "substitution" or iso.upper() in DEFAULT_SUBST_ISO
    ) and biomass == "lhs":
        return sql.iso_total_primary_energy_subs_lhs(iso, year, reg_short)
    if pe_accounting == "substitution" and biomass == "rhs":
        return sql.iso_total_primary_energy_subs_rhs(iso, year, reg_short)
    if pe_accounting != "substitution" and biomass == "rhs":
        return sql.iso_total_primary_energy_dir_rhs(iso, year, reg_short)
    return sql.iso_total_primary_energy_dir_lhs(iso, year, reg_short)


def regional_total_pe_sql(
    sql,
    reg_short: str,
    year: int,
    full_reg: str,
    *,
    pe_accounting: str = DEFAULT_PE_ACCOUNTING,
    biomass: str = DEFAULT_BIOMASS,
) -> str:
    """Match ``AddPolicies._retrieve_regional_total_primary_energy``."""
    if pe_accounting == "substitution" and biomass == "lhs":
        return sql.regional_total_primary_energy_subs_lhs(reg_short, year)
    if (
        pe_accounting == "substitution" or full_reg in DEFAULT_SUBST_REG
    ) and biomass == "rhs":
        return sql.regional_total_primary_energy_subs_lhs(reg_short, year)
    if pe_accounting != "substitution" and biomass == "rhs":
        return sql.regional_total_primary_energy_dir_rhs(reg_short, year)
    return sql.regional_total_primary_energy_dir_lhs(reg_short, year)


def iso_total_eg_sql(sql, iso: str, year: int, reg_short: str) -> str:
    """Match ``AddPolicies._retrieve_iso_total_electricty_generation``."""
    raise NotImplementedError("Eg country totals not wired yet")


def regional_total_eg_sql(sql, reg_short: str, year: int, full_reg: str) -> str:
    """Match ``AddPolicies._retrieve_regional_total_elec_generation``."""
    raise NotImplementedError("Eg regional totals not wired yet")


def iso_total_fe_sql(sql, iso: str, year: int, reg_short: str) -> str:
    """Match ``AddPolicies._retrieve_iso_total_final_energy``."""
    raise NotImplementedError("Fe country totals not wired yet")


def regional_total_fe_sql(sql, reg_short: str, year: int, full_reg: str) -> str:
    """Match ``AddPolicies._retrieve_regional_total_final_energy``."""
    raise NotImplementedError("Fe regional totals not wired yet")


_HANDLERS = {
    "Pe": (iso_total_pe_sql, regional_total_pe_sql, "pe"),
    "Eg": (iso_total_eg_sql, regional_total_eg_sql, "eg"),
    "Fe": (iso_total_fe_sql, regional_total_fe_sql, "fe"),
}


def retrieve_energy_shares(
    basis: str = "Pe",
    year: int = 2015,
    region_id: str = "R12",
) -> pd.DataFrame:
    """Return country energy and country-in-region weight for `basis`.

    Parameters
    ----------
    basis :
        ``Pe``, ``Eg``, or ``Fe``.
    year :
        IEA reference year.
    region_id :
        Node codelist ID (default ``R12``).
    """
    try:
        iso_sql_fn, reg_sql_fn, col_prefix = _HANDLERS[basis]
    except KeyError:
        raise ValueError(
            f"Unknown basis {basis!r}; choose from {tuple(_HANDLERS)}"
        ) from None

    mapping = load_iso_mapping(region_id)
    target_regs = set(mapping["region"].unique())

    conn, sql, meta = connect_iea(region_id)
    curs = conn.cursor()
    rows = []
    try:
        reg_tot = {
            full_reg: run_sql(
                curs, reg_sql_fn(sql, full_reg.split("_", 1)[1], year, full_reg)
            )
            for full_reg in sorted(target_regs)
        }
        for full_reg, tot in reg_tot.items():
            log.info("%s Tot%s(%s) = %.6g", full_reg, basis, year, tot)

        for iso, row in mapping.iterrows():
            full_reg = row["region"]
            val = run_sql(curs, iso_sql_fn(sql, iso, year, full_reg.split("_", 1)[1]))
            tot = reg_tot[full_reg]
            rows.append(
                {
                    "iso": iso,
                    "country": row["country"],
                    "region": full_reg,
                    "energy_basis": basis,
                    "year": year,
                    f"iso_{col_prefix}": val,
                    f"region_{col_prefix}": tot,
                    "country_in_region_weight": (val / tot) if tot else float("nan"),
                    "iea_revision": meta["IEA_revision"],
                    "iea_table": meta["IEA_table"],
                }
            )
    finally:
        curs.close()
        conn.close()

    df = pd.DataFrame(rows).sort_values(
        ["region", "country_in_region_weight"], ascending=[True, False]
    )
    check = df.groupby("region")["country_in_region_weight"].sum()
    log.info(
        "Sum of country-in-region %s weights "
        "(expect ~1 if IEA coverage is complete):\n%s",
        basis,
        check.to_string(),
    )
    return df
