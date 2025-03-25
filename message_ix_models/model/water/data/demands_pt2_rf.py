
from message_ix_models.model.water.data.demand_rules import WATER_AVAILABILITY, SHARE_CONSTRAINTS_RECYCLING, SHARE_CONSTRAINTS_GW, eval_field

def _preprocess_availability_data(df: pd.DataFrame, monthly: bool = False, df_x: pd.DataFrame = None, info = None) -> pd.DataFrame:
    """
    Preprocesses availability data

    Parameters
    ----------
    df : pd.DataFrame
    monthly : bool
    df_x : pd.DataFrame
    info : Context

    Returns
    -------
    df : pd.DataFrame
    """
    df.drop(["Unnamed: 0"], axis=1, inplace=True)
    df.index = df_x["BCU_name"].index
    df = df.stack().reset_index()
    df.columns = pd.Index(["Region", "years", "value"])
    df.fillna(0, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df["year"] = pd.DatetimeIndex(df["years"]).year
    df["time"] = "year" if not monthly else pd.DatetimeIndex(df["years"]).month
    df["Region"] = df["Region"].map(df_x["BCU_name"])
    df2210 = df[df["year"] == 2100].copy()
    df2210["year"] = 2110
    df = pd.concat([df, df2210])
    df = df[df["year"].isin(info.Y)]
    return df

def read_water_availability(context: "Context") -> Sequence[pd.DataFrame]:
    """
    Reads water availability data and bias correct
    it for the historical years and no climate
    scenario assumptions.

    Parameters
    ----------
    context : .Context

    Returns
    -------
    data : (pd.DataFrame, pd.DataFrame)
    """

    # Reference to the water configuration
    info = context["water build info"]
    # reading sample for assiging basins
    PATH = package_data_path(
        "water", "delineation", f"basins_by_region_simpl_{context.regions}.csv"
    )
    df_x = pd.read_csv(PATH)
    monthly = False
    match context.time:
        case "year":
            # path for reading basin delineation file
            path1 = package_data_path(
                "water",
                "availability",
                f"qtot_5y_{context.RCP}_{context.REL}_{context.regions}.csv",
            )
            # Reading data, the data is spatially and temprally aggregated from GHMs
            path2 = package_data_path(
                "water",
                "availability",
                f"qr_5y_{context.RCP}_{context.REL}_{context.regions}.csv",
            )
        case "month":
            monthly = True
            path1 = package_data_path(
                "water",
                "availability",
                f"qtot_5y_m_{context.RCP}_{context.REL}_{context.regions}.csv",
            )

            # Reading data, the data is spatially and temporally aggregated from GHMs
            path2 = package_data_path(
                "water",
                "availability",
                f"qr_5y_m_{context.RCP}_{context.REL}_{context.regions}.csv",
            )
        case _:
            raise ValueError(f"Invalid time period: {context.time}")

    df_sw = pd.read_csv(path1)
    df_sw = _preprocess_availability_data(df_sw, monthly=monthly, df_x = df_x, info = info)

    df_gw = pd.read_csv(path2)
    df_gw = _preprocess_availability_data(df_gw, monthly=monthly, df_x = df_x, info = info)

    return df_sw, df_gw


def add_water_availability(context: "Context") -> dict[str, pd.DataFrame]:
    """
    Adds water supply constraints

    Parameters
    ----------
    context : .Context

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'. Values
        are data frames ready for :meth:`~.Scenario.add_par`.
    """

    # define an empty dictionary
    results = {}
    # Adding freshwater supply constraints
    # Reading data, the data is spatially and temprally aggregated from GHMs

    df_sw, df_gw = read_water_availability(context)
    water_availability = []
    for rule in WATER_AVAILABILITY:
        match rule["df_source"]:
            case "df_sw":
                df_source = df_sw
            case "df_gw":
                df_source = df_gw
            case _:
                raise ValueError(f"Invalid df_source: {rule['df_source']}")

        dmd_df = make_df(
            rule["type"],
            node="B"+ eval_field(rule["node"], df_source),
            commodity=rule["commodity"],
            level=rule["level"],
            year=eval_field(rule["year"], df_source),
            time=eval_field(rule["time"], df_source),
            value= -eval_field(rule["value"], df_source),
            unit=rule["unit"],
        )
        water_availability.append(dmd_df)

    dmd_df = pd.concat(water_availability)

    dmd_df["value"] = dmd_df["value"].apply(lambda x: x if x <= 0 else 0)

    results["demand"] = dmd_df


    share_constraints_gw = []
    for rule in SHARE_CONSTRAINTS_GW:
        # share constraint lower bound on groundwater
        if rule["df_source1"] == "df_gw" and rule["df_source2"] == "df_sw":
            df_source1 = df_gw
            df_source2 = df_sw
        else:
            raise ValueError(f"Invalid df_source: {rule['df_source1']} or {rule['df_source2']}")
        
        df_share = make_df(
            rule["type"],
            shares=rule["shares"],
            node_share="B" + eval_field(rule["node"], df_source1),
            year_act=eval_field(rule["year"], df_source1),
            time=eval_field(rule["time"], df_source1),
            value=eval_field(rule["value"], df_source1, df_source2),             
            unit=rule["unit"],
    )

    df_share["value"] = df_share["value"].fillna(0)

    results["share_commodity_lo"] = df_share

    return results


def add_irrigation_demand(context: "Context") -> dict[str, pd.DataFrame]:
    """
    Adds endogenous irrigation water demands from GLOBIOM emulator

    Parameters
    ----------
    context : .Context

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'. Values
        are data frames ready for :meth:`~.Scenario.add_par`.
    """
    # define an empty dictionary
    results = {}

    scen = context.get_scenario()
    # add water for irrigation from globiom
    land_out_1 = scen.par(
        "land_output", {"commodity": "Water|Withdrawal|Irrigation|Cereals"}
    )
    land_out_1["level"] = "irr_cereal"
    land_out_2 = scen.par(
        "land_output", {"commodity": "Water|Withdrawal|Irrigation|Oilcrops"}
    )
    land_out_2["level"] = "irr_oilcrops"
    land_out_3 = scen.par(
        "land_output", {"commodity": "Water|Withdrawal|Irrigation|Sugarcrops"}
    )
    land_out_3["level"] = "irr_sugarcrops"

    land_out = pd.concat([land_out_1, land_out_2, land_out_3])
    land_out["commodity"] = "freshwater"

    land_out["value"] = 1e-3 * land_out["value"]

    # take land_out edited and add as a demand in  land_input
    results["land_input"] = land_out

    return results
