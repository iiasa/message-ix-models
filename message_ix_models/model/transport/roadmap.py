"""Prepare Africa data from the ICCT Roadmap 1.0 model.

The data are located in :data:`FILE`.

The countries belonging to the Africa region in the Roadmap 1.0 model are:
    Angola, Burundi, Benin, Burkina Faso, Botswana, Central African Republic,
    Ivory Coast, Cameroon, Democratic Republic of the Congo, Congo, Comoros, Cape Verde,
    Djibouti, Algeria, Eritrea, Ethiopia, Gabon, Ghana, Guinea, Gambia,
    Guinea-Bissau, Equatorial Guinea, Kenya, Liberia, Libyan Arab Jamahiriya, Lesotho,
    Morocco, Madagascar, Mali, Mozambique, Mauritania, Mauritius, Malawi, Namibia,
    Niger, Nigeria, Rwanda, Sudan, Senegal, Sierra Leone, Somalia, Sao Tome and
    Principe, Swaziland, Seychelles, Chad, Togo, Tunisia, United Republic of Tanzania,
    Uganda, South Africa, Zambia, Zimbabwe -- see sheet **Countries by Roadmap
    Region** in RoadmapResults_2017.xlsx.
"""

import pandas as pd
from plotnine import save_as_pdf_pages

from message_ix_models.util import package_data_path

#: Name of the file containing the data.
FILE = "RoadmapResults_2017.xlsx"

#: Historical years from the Roadmap model.
HIST_YEARS = [2000, 2005, 2010, 2015]

#: All Roadmap model timesteps.
ALL_YEARS = [2000, 2005, 2010, 2015, 2020, 2025, 2030, 2035, 2040, 2045, 2050]

COLUMN_MAP = {
    "Mode": "Mode/vehicle type",
}

MODE_MAP = {
    "Buses": "Bus",
    "Freight Rail": "Freight trains",
    "Aviation": "Domestic passenger airplanes",
    "LDV": "Cars/light trucks",
    "Passenger Rail": "Passenger trains",
    "LHDT": "Freight trucks",
    "MHDT_HHDT": "Freight trucks",
}

VAR_MAP = dict(
    # Map of ICCT's Roadmap variables to IEA's EEI variable names
    Stock_million="Stock (10^6 vehicle)",
    Sales_million="New vehicle sales (10^6 vehicle)",
    VKT_billion="Vehicle kilometre per year (10^6 vkm/yr)",
    TKM_billion="Tonne kilometre per year (10^6 tkm/yr)",
    PKM_billion="Passenger kilometre per year (10^6 pkm/yr)",
    # TODO IEA EEI's is in PJ, adjust those values in future plots
    Energy_PJ="Total final energy (EJ/yr)",
    TTW_CO2_Mt="Total final emissions (MtCO2/yr)",
    # TODO add PM2.5 to IAMconsortium/units: https://github.com/IAMconsortium/units
    TTW_PM2_5_kt="Total final PM2.5 emissions (ktPM2.5/yr)",
)

UNITS = dict(
    # Appearing in input file
    Stock_million=(None, "megavehicle", "megavehicle"),
    Sales_million=(None, "megavehicle", "megavehicle"),
    VKT_billion=(None, "gigavehicle kilometer / year", "gigavehicle kilometer / year"),
    TKM_billion=(None, "gigatonne kilometre / year", "gigatonne kilometre / year"),
    PKM_billion=(
        None,
        "gigapassenger kilometre / year",
        "gigapassenger kilometre / year",
    ),
    Energy_PJ=(None, "PJ", "EJ"),
    TTW_CO2_Mt=(None, "Megatonne CO2", "Megatonne CO2"),
    # TODO add PM2.5 to IAMconsortium/units: https://github.com/IAMconsortium/units
    # TTW_PM2_5_kt=(None, "kilotonne PM2.5", "kilotonne PM2.5"),
    # occupancy=(None, "pkm / vkm", "pkm / vkm"),
    # vkm_capita=(None, "vkm / capita", "vkm / capita"),
    # energy_vkm=(None, "MJ / vkm", "MJ / vkm"),
    # mileage=(1000.0, "vkm / vehicle", "vkm / vehicle"),
    # tkm_capita=(1000.0, "tonne kilometre / capita", "tonne kilometre / capita"),
    # energy_tkm=(None, "MJ / tkm", "MJ / tkm"),
    # load_factor=(None, "tkm / vkm", "tkm / vkm"),
    # # Created below
    # activity=(None, None, "gigapassenger kilometre / year"),
)


def get_roadmap_data(
    context, region=("Africa", "R11_AFR"), years=None, plot=False
) -> pd.DataFrame:
    """Read transport activity data for Africa.

    The data is read from ``RoadmapResults_2017.xlsx``, which is already aggregated
    into total values for the Africa region -including the countries mentioned above.
    It is then processed into a DataFrame format compatible with the IEA's EEI
    datasets, to be ultimately used for **MESSAGEix-Transport** scenario calibration.

    The region name returned is :class:`~message-ix-models.util.context.Context`
    dependent. However, it is just relevant for ``R11``, ```R12`` or ``R14`` regional
    aggregations, since the country-level aggregation is the same in both cases.

    By default, it processes the historical years from the Roadmap model, up to 2015.
    The source file also contains projections up to 2050 (in 5-year time steps),
    and can be retrieved through the ``years`` argument.

    Parameters
    ----------
    context : .Context
        Information about target Scenario.
    region : tuple
        (Roadmap region, MESSAGEix region). Info about the target region. At the
        moment, default values are set to ``Africa`` and ``R11`` since these are of
        relevance for the current research of MESSAGEix-Transport.
    years : list
        Default: ``None``, implies that it retrieves exclusively the historical time
        steps from the Roadmap model. It can provide projections up to 2050 if other
        lists of time steps are provided.
    plot : bool, optional
        If ``True``, plots per mode will be generated in folder /debug.

    Returns
    -------
    DataFrame : pandas.DataFrame
        Same format as returned by
        :func:`~message_ix_models.tools.iea.eei.get_eei_data`.
    """
    # Load and process data for Africa
    # Check years provided
    if not years:
        years = HIST_YEARS
    else:
        for x in years:
            assert x in ALL_YEARS
    # Read xlsx file
    df = pd.read_excel(
        package_data_path("transport", FILE), sheet_name="Model Results", header=0
    )
    df = df[(df["Year"].isin(years)) & (df["Roadmap_Region"] == region[0])].reset_index(
        drop=True
    )

    # Map ICCT modes to IEA EEI's modes
    # Sum up 2_3_W modes that represent disaggregated values for 2 and 3 wheelers. Same
    # for MHDT_HHDT, which two sub-categories are also accounted separately
    df = (
        df.replace({"Mode": MODE_MAP})
        .groupby(by=["Year", "Mode"], as_index=False)
        .sum(numeric_only=True)
        .sort_values(by=["Mode", "Year"])
        .melt(id_vars=["Mode", "Year"], value_name="Value")
        .replace({"variable": VAR_MAP})
        .rename(columns={"Mode": "Mode/vehicle type"})
        .rename(columns=lambda c: c.lower())
        .reset_index(drop=True)
    )

    df = pd.concat(
        [
            df.drop("variable", axis=1),
            # Split "variable" and "units" columns
            df["variable"].str.extract(r"(?P<variable>.*) \((?P<units>.*)\)"),
        ],
        axis=1,
    )

    # Get the input regional aggregation and add it as a column
    df["region"] = region[1]

    if plot:
        # Path for debug output
        debug_path = context.get_local_path("debug")
        debug_path.mkdir(parents=True, exist_ok=True)
        # Plot all indicators as grid, per mode, and store them into PNG images
        save_as_pdf_pages(
            # plot_params_per_mode(df),  # TODO Convert to use genno.core.plotnine.Plot
            [],
            filename=f"{context.model.regions}_AFR_Indicators_per_mode.pdf",
            path=debug_path,
        )

    return df
