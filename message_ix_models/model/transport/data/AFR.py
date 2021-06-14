"""Prepare Africa data from the Roadmap 1.0 model via RoadmapResults_2017.xlsx.

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
from collections import defaultdict

import pandas as pd
from iam_units import registry
from message_ix_models.util import broadcast, eval_anno, private_data_path, same_node

FILE = "RoadmapResults_2017.xlsx"

HIST_YEARS = [2000, 2005, 2010, 2015]


def get_afr_data():
    """Read transport activity data for Africa.

    The data is read from RoadmapResults_2017.xlsx. It is then processed into a
    format compatible with the IEA's EEI datasets, to be used altogether for
    MESSAGEix-Transport scenario calibration.

    Returns
    -------
    DataFrame : pandas.DataFrame
        Same format as returned by :func:`~message_data.tools.iea_eei.get_eei_data`.
    """
    # Load and process data for Africa
    # Read xlsx file
    df = pd.read_excel(
        private_data_path("transport", FILE), sheet_name="Model Results", header=0
    )
    df = df[
        (df["Year"].isin(HIST_YEARS)) & (df["Roadmap_Region"] == "Africa")
    ].reset_index(drop=True)

    # Sum up 2_3_W modes that represent disaggregated values for 2 and 3 wheelers. Same
    # for MHDT_HHDT, which two sub-categories are also accounted separately
    df = df.groupby(by=["Year", "Mode"], as_index=False).sum().sort_values(by=[
        "Mode", "Year"])

    return df


