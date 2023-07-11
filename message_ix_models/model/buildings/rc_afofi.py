"""Compute "AFOFI" share of "RC".

This code copied 2023-07-11 from iiasa/MESSAGE_Buildings @ 7fb0e6fd.
"""
from typing import Tuple

import pandas as pd
from jaydebeapi import connect


def return_PERC_AFOFI() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Query the ECE IEA database and return the share of AFOFI in RC."""

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
