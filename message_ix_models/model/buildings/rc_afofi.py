"""Compute AFOFI shares of RC.

**AFOFI** is Agriculture, FOrestry, and FIsheries; **RC** is Residential and Commercial.

In brief: the MESSAGEix-GLOBIOM "sector" (or group of sectors) referred to as "rc" or
"RC" is inclusive of not only residential and commercial, but also agricultural,
forestry, fisheries, and some other sectors.

When linking the buildings models (ACCESS, STURM) to MESSAGEix-GLOBIOM, the specific
residential and commercial sectors are represented by those models, but the remainder of
the composite "RC" sector, i.e. AFOFI, must still be represented in MESSAGE.

These functions give a share of AFOFI in RC for demands of the "rc_therm" and "rc_spec"
commodities; or activities of the technologies producing these commodities.
:func:`return_PERC_AFOFI` was brought into :mod:`message_data` from the (private,
unpublished) iiasa/MESSAGE_Buildings repository.
"""

from collections.abc import Iterable
from functools import lru_cache

import pandas as pd
from genno import Quantity
from jaydebeapi import connect

__all__ = [
    "get_afofi_commodity_shares",
    "get_afofi_technology_shares",
]


def get_afofi_commodity_shares() -> Quantity:
    """Wrap MESSAGE_Buildings code that queries the ECE IEA database.

    Returns
    -------
    Quantity
        with dimensions (n, c); c including "rc_spec" and "rc_therm".
    """
    from .rc_afofi import return_PERC_AFOFI

    # Invoke the function
    therm, spec = return_PERC_AFOFI()

    # - Rename dimensions
    # - Prepend "R12_" to node codes.
    # - Use "rc_therm" or "rc_spec" for the original tech to which the share is
    # - applicable
    dfs = [
        df.rename_axis("n", axis=0)
        .rename_axis("c", axis=1)
        .rename(index=lambda s: f"R12_{s}", columns={"perc_afofi": f"rc_{name}"})
        for name, df in (("therm", therm), ("spec", spec))
    ]

    # - Combine to a single pd.Series with multi-index
    # - Convert to Quantity
    return Quantity(pd.concat(dfs).stack(), name="afofi share")


def get_afofi_technology_shares(
    c_shares: Quantity, technologies: Iterable[str]
) -> Quantity:
    """Compute AFOFI shares by technology from shares by commodity.

    Returns
    -------
    Quantity
        with dimensions (n, t); t including all of `technologies`.
    """
    from genno.operator import aggregate, mul, rename_dims

    agg = {}
    weight = {}

    for t in technologies:
        agg[t] = {
            "h2_fc_RC": ["rc_spec", "rc_therm"],
            "sp_el_RC": ["rc_spec"],
        }.get(t, ["rc_therm"])
        weight[t] = {"h2_fc_RC": 0.5}.get(t, 1.0)

    return (
        c_shares.pipe(rename_dims, {"c": "t"})  # type: ignore [attr-defined]
        .pipe(aggregate, {"t": agg}, keep=False)
        .pipe(mul, Quantity(pd.Series(weight).rename_axis("t")))
    )


@lru_cache
def return_PERC_AFOFI() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Query the ECE IEA database and return the share of AFOFI in RC.

    This function copied 2023-07-11 from iiasa/MESSAGE_Buildings @ 7fb0e6fd.

    .. todo:: Describe which data from the IEA data base are used for the calculation,
       for instance by adding comments to the code.

    Returns
    -------
    tuple (pandas.DataFrame, pandas.DataFrame)
        - The first data frame pertains to rc_therm, the second to rc_spec.
        - Index axis named "regions", with indices like "AFR". These are the R12 nodes,
          except the codes do not match codes like "R12_AFR" in the scenario. The code
          does not support other codelists.
        - Columns axis named "flow_code", with a single column named "perc_afofi".
        - Values like 0.152, i.e. **shares**: note this contradicts "perc"ent in the
          function and column name, which would suggest percent values like 15.2.
    """

    # Connect to database
    conn = connect(
        "oracle.jdbc.driver.OracleDriver",
        "jdbc:oracle:thin:@x8oda.iiasa.ac.at:1521/pGP3.iiasa.ac.at",
        ["iea", "iea"],
        "./ojdbc6.jar",
    )
    curs = conn.cursor()

    # Statement to get GDP and Population Projections
    statement = " select value, country_code, prod_code, flow_code, unit, rev_code, \
        ryear \
        from edb_data_2017 \
        where prod_code in (62,67) \
        and flow_code in (77,76,75) \
        and rev_code=2017 "

    curs.execute(statement)
    rescom = pd.DataFrame(
        curs.fetchall(),
        columns=[
            "value",
            "country_code",
            "prod_code",
            "flow_code",
            "unit",
            "rev_code",
            "year",
        ],
    )

    statement = " select region, country_code from \
                 edb_region where scheme in ('MESSAGE')"
    curs.execute(statement)
    countryCode_data = pd.DataFrame(curs.fetchall(), columns=["region", "country_code"])

    statement = " select * from edb_prod"
    curs.execute(statement)
    propCode_data = pd.DataFrame(curs.fetchall(), columns=["prod_code", "prod_name"])

    statement = " select * from edb_flow"
    curs.execute(statement)
    flowCode_data = pd.DataFrame(curs.fetchall(), columns=["flow_code", "flow_name"])

    rescom = (
        rescom.merge(countryCode_data, on="country_code")
        .merge(propCode_data, on="prod_code")
        .merge(flowCode_data, on="flow_code")
    )

    rescom = rescom.loc[
        rescom["region"].isin(
            [
                "AFR",
                "CPA",
                "EEU",
                "FSU",
                "LAM",
                "MEA",
                "NAM",
                "PAO",
                "PAS",
                "SAS",
                "WEU",
            ]
        )
    ]

    # Separate CHN
    rescom["region"].loc[rescom["country_code"] == 1] = "CHN"
    rescom["region"].loc[rescom["region"] == "CPA"] = "RCPA"

    rescom = (
        rescom.groupby(["region", "year", "flow_code", "prod_name"]).sum().reset_index()
    )
    rescom = rescom.pivot(
        index=["region", "year", "flow_code"], columns="prod_name", values="value"
    ).reset_index()
    rescom["therm"] = rescom["total"] - rescom["electricity"]

    rc_spec = rescom.pivot(
        index=["region", "year"], columns="flow_code", values="electricity"
    ).reset_index()
    rc_spec = rc_spec.rename(columns={75: "rctot", 76: "resid", 77: "comm"})

    rc_therm = rescom.pivot(
        index=["region", "year"], columns="flow_code", values="therm"
    ).reset_index()
    rc_therm = rc_therm.rename(columns={75: "rctot", 76: "resid", 77: "comm"})

    rc_spec["afofi"] = rc_spec["rctot"] - rc_spec["resid"] - rc_spec["comm"]
    rc_therm["afofi"] = rc_therm["rctot"] - rc_therm["resid"] - rc_therm["comm"]

    rc_spec["perc_afofi"] = rc_spec["afofi"] / rc_spec["rctot"]
    rc_therm["perc_afofi"] = rc_therm["afofi"] / rc_therm["rctot"]

    # # Visual inspection
    # import matplotlib.pyplot as plt

    # fig, ax = plt.subplots(figsize=(8, 6))
    # for label, df in rc_spec.loc[rc_spec["year"] >= 1990].groupby("region"):
    #     df.plot(x="year", y="perc_afofi", ax=ax, label=label)

    # plt.show()

    # fig, ax = plt.subplots(figsize=(8, 6))
    # for label, df in rc_therm.loc[rc_therm["year"] >= 1990].groupby("region"):
    #     df.plot(x="year", y="perc_afofi", ax=ax, label=label)

    # plt.show()

    # I'll just keep the average from 2010 to 2015, looks quite stable to me!

    perc_afofi_therm = (
        rc_therm.loc[rc_therm["year"] >= 2010, ["region", "perc_afofi"]]
        .groupby("region")
        .mean()
    )
    perc_afofi_spec = (
        rc_spec.loc[rc_spec["year"] >= 2010, ["region", "perc_afofi"]]
        .groupby("region")
        .mean()
    )

    return perc_afofi_therm, perc_afofi_spec
