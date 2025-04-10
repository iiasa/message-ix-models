def get_weo_regional_differentiation(config: "Config") -> pd.DataFrame:
    """Apply WEO regional differentiation.

    1. Retrieve WEO data using :func:`.get_weo_data`.
    2. Map data to MESSAGEix-GLOBIOM regions according to the :attr:`.Config.node`.
    3. Calculate cost ratios for each region relative to the
       :attr:`~.Config.ref_region`.

    Parameters
    ----------
    config : .Config
        The function responds to the fields:
        :attr:`~.Config.base_year`,
        :attr:`~.Config.node`, and
        :attr:`~.Config.ref_region`.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - message_technology: MESSAGEix technology name
        - region: MESSAGEix region
        - weo_ref_region_cost: WEO cost in reference region
        - reg_cost_ratio: regional cost ratio relative to reference region
    """

    # Grab WEO data and keep only investment costs
    df_weo = get_weo_data()

    # Even if config.base_year is greater than 2022, use 2022 WEO values
    sel_year = str(2022)
    log.info("…using year " + str(sel_year) + " data from WEO")

    # - Retrieve a map from MESSAGEix node IDs to WEO region names.
    # - Map WEO data to MESSAGEix regions.
    # - Keep only base year data.
    l_sel_weo = []
    for message_node, weo_region in get_weo_region_map(config.node).items():
        df_sel = (
            df_weo.query("year == @sel_year & weo_region == @weo_region")
            .assign(region=message_node)
            .rename(columns={"value": "weo_cost"})
            .reindex(
                [
                    "cost_type",
                    "weo_technology",
                    "weo_region",
                    "region",
                    "year",
                    "weo_cost",
                ],
                axis=1,
            )
        )

        l_sel_weo.append(df_sel)
    df_sel_weo = pd.concat(l_sel_weo)

    # If specified reference region is not in WEO data, then give error
    assert config.ref_region is not None
    ref_region = config.ref_region.upper()
    if ref_region not in df_sel_weo.region.unique():
        raise ValueError(
            f"Reference region {ref_region} not found in WEO data. "
            "Please specify a different reference region. "
            f"Available regions are: {df_sel_weo.region.unique()}"
        )

    # Calculate regional investment cost ratio relative to reference region
    df_reg_ratios = (
        df_sel_weo.query("region == @ref_region and cost_type == 'inv_cost'")
        .rename(columns={"weo_cost": "weo_ref_region_cost"})
        .drop(columns={"weo_region", "region"})
        .merge(
            df_sel_weo.query("cost_type == 'inv_cost'"), on=["weo_technology", "year"]
        )
        .assign(reg_cost_ratio=lambda x: x.weo_cost / x.weo_ref_region_cost)
        .reindex(
            [
                "weo_technology",
                "region",
                "weo_ref_region_cost",
                "reg_cost_ratio",
            ],
            axis=1,
        )
    )

    # Calculate fixed O&M cost ratio relative to investment cost
    # Get investment costs
    df_inv = (
        df_sel_weo.query("cost_type == 'inv_cost' and year == @sel_year")
        .rename(columns={"weo_cost": "inv_cost"})
        .drop(columns=["year", "cost_type"])
    )

    # Get fixed O&M costs
    df_fix = (
        df_sel_weo.query("cost_type == 'fix_cost' and year == @sel_year")
        .rename(columns={"weo_cost": "fix_cost"})
        .drop(columns=["year", "cost_type"])
    )

    # Merge investment and fixed O&M costs
    # Calculate ratio of fixed O&M costs to investment costs
    df_fom_inv = (
        df_inv.merge(df_fix, on=["weo_technology", "weo_region", "region"])
        .assign(weo_fix_ratio=lambda x: x.fix_cost / x.inv_cost)
        .drop(columns=["inv_cost", "fix_cost", "weo_region"])
    )

    # Combine cost ratios (regional and fix-to-investment) together
    df_cost_ratios = df_reg_ratios.merge(df_fom_inv, on=["weo_technology", "region"])

    return df_cost_ratios


def get_weo_regional_differentiation_vectorized(config: "Config") -> pd.DataFrame:
    """Apply WEO regional differentiation.

    1. Retrieve WEO data using :func:`.get_weo_data`.
    2. Map data to MESSAGEix-GLOBIOM regions according to the :attr:`.Config.node`.
    3. Calculate cost ratios for each region relative to the
       :attr:`~.Config.ref_region`.

    Parameters
    ----------
    config : .Config
        The function responds to the fields:
        :attr:`~.Config.base_year`,
        :attr:`~.Config.node`, and
        :attr:`~.Config.ref_region`.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - message_technology: MESSAGEix technology name
        - region: MESSAGEix region
        - weo_ref_region_cost: WEO cost in reference region
        - reg_cost_ratio: regional cost ratio relative to reference region
        - weo_fix_ratio: fixed O&M cost to investment cost ratio
    """
    import pandas as pd  # in case not already imported

    # Retrieve full data and filter to the selected year.
    df_weo = get_weo_data()
    sel_year = "2022"
    log.info("…using year " + sel_year + " data from WEO")

    # Map MESSAGEix region IDs to WEO regions.
    mapping = get_weo_region_map(config.node)
    map_df = pd.DataFrame(list(mapping.items()), columns=["region", "weo_region"])

    # Filter WEO data for the selected year and merge with mapping.
    df_weo_sel = df_weo[df_weo["year"] == sel_year].copy()
    df_sel_weo = map_df.merge(df_weo_sel, on="weo_region", how="inner")
    df_sel_weo.rename(columns={"value": "weo_cost"}, inplace=True)

    # Restrict only to cost types of interest.
    df_sel_weo = df_sel_weo[df_sel_weo["cost_type"].isin(["inv_cost", "fix_cost"])]

    # Verify the specified reference region is contained in the data.
    assert config.ref_region is not None
    ref_region = config.ref_region.upper()
    if ref_region not in df_sel_weo["region"].unique():
        raise ValueError(
            f"Reference region {ref_region} not found in WEO data. "
            "Please specify a different reference region. "
            f"Available regions are: {df_sel_weo['region'].unique()}"
        )

    # Pivot table so that each row has both investment and fixed O&M costs.
    pivot_df = df_sel_weo.pivot_table(
        index=["weo_technology", "weo_region", "region", "year"],
        columns="cost_type",
        values="weo_cost",
        aggfunc="first",  # assuming there is only one unique entry per group
    ).reset_index()

    # Calculate the fixed O&M to investment cost ratio.
    pivot_df["weo_fix_ratio"] = pivot_df["fix_cost"] / pivot_df["inv_cost"]

    # Compute the regional investment cost ratio.
    # Extract reference investment cost data using the reference region.
    ref_cost = pivot_df[pivot_df["region"] == ref_region][
        ["weo_technology", "year", "inv_cost"]
    ].rename(columns={"inv_cost": "weo_ref_region_cost"})
    # Merge the reference cost back with the pivoted data.
    result_df = pivot_df.merge(ref_cost, on=["weo_technology", "year"], how="left")
    result_df["reg_cost_ratio"] = (
        result_df["inv_cost"] / result_df["weo_ref_region_cost"]
    )

    # Keep only required result columns.
    result_df = result_df[
        [
            "weo_technology",
            "region",
            "weo_ref_region_cost",
            "reg_cost_ratio",
            "weo_fix_ratio",
        ]
    ]

    return result_df
