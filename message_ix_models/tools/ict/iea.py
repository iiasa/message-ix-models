from typing import TYPE_CHECKING

import camelot
import pandas as pd

from message_ix_models.util import pycountry

if TYPE_CHECKING:
    from pathlib import Path


iea_weo_map = {
    "Africa": ["North Africa", "Sub-Saharan Africa"],
    "Asia Pacific": [
        "Southeast Asia",
        "Australia",
        "Bangladesh",
        "Democratic People’s Republic of Korea (North Korea)",
        "India",
        "Japan",
        "Korea",
        "Mongolia",
        "Nepal",
        "New Zealand",
        "Pakistan",
        "The People’s Republic of China (China)",
        "Sri Lanka",
        "Chinese Taipei",
        "and other Asia Pacific countries and territories",
    ],
    "Caspian": [
        "Armenia",
        "Azerbaijan",
        "Georgia",
        "Kazakhstan",
        "Kyrgyzstan",
        "Tajikistan",
        "Turkmenistan",
        "Uzbekistan",
    ],
    "Central and South America": [
        "Argentina",
        "Plurinational State of Bolivia (Bolivia)",
        "Bolivarian Republic of Venezuela (Venezuela)",
        "Brazil",
        "Chile",
        "Colombia",
        "Costa Rica",
        "Cuba",
        "Curaçao",
        "Dominican Republic",
        "Ecuador",
        "El Salvador",
        "Guatemala",
        "Guyana",
        "Haiti",
        "Honduras",
        "Jamaica",
        "Nicaragua",
        "Panama",
        "Paraguay",
        "Peru",
        "Suriname",
        "Trinidad and Tobago",
        "Uruguay",
        "and other Central and South American countries and territories",
    ],
    "China": ["China", "Hong Kong"],
    "Eurasia": ["Caspian", "the Russian Federation (Russia)"],
    "Europe": [
        "European Union",
        "Albania",
        "Belarus",
        "Bosnia and Herzegovina",
        "Gibraltar",
        "Iceland",
        "Israel",
        "Kosovo",
        "Montenegro",
        "North Macedonia",
        "Norway",
        "Republic of Moldova",
        "Serbia",
        "Switzerland",
        "Türkiye",
        "Ukraine",
        "United Kingdom",
    ],
    "European Union": [
        "Austria",
        "Belgium",
        "Bulgaria",
        "Croatia",
        "Cyprus,",
        "Czech Republic",
        "Denmark",
        "Estonia",
        "Finland",
        "France",
        "Germany",
        "Greece",
        "Hungary",
        "Ireland",
        "Italy",
        "Latvia",
        "Lithuania",
        "Luxembourg",
        "Malta",
        "Netherlands",
        "Poland",
        "Portugal",
        "Romania",
        "Slovak Republic",
        "Slovenia",
        "Spain",
        "Sweden",
    ],
    "Middle East": [
        "Bahrain",
        "Islamic Republic of Iran (Iran)",
        "Iraq",
        "Jordan",
        "Kuwait",
        "Lebanon",
        "Oman",
        "Qatar",
        "Saudi Arabia",
        "Syrian Arab Republic (Syria)",
        "United Arab Emirates",
        "Yemen",
    ],
    "North Africa": ["Algeria", "Egypt", "Libya", "Morocco", "Tunisia"],
    "North America": ["Canada", "Mexico", "United States"],
    "Southeast Asia": [
        "Brunei Darussalam",
        "Cambodia",
        "Indonesia",
        "Lao People’s Democratic Republic (Lao PDR)",
        "Malaysia",
        "Myanmar",
        "Philippines",
        "Singapore",
        "Thailand",
        "Viet Nam",
        "Timor Leste",
    ],
    "Sub-Saharan Africa": [
        "Angola",
        "Benin",
        "Botswana",
        "Cameroon",
        "Côte d’Ivoire",
        "Democratic Republic of the Congo (DRC)",
        "Equatorial Guinea",
        "Eritrea",
        "Ethiopia",
        "Gabon",
        "Ghana",
        "Kenya",
        "Kingdom of Eswatini",
        "Madagascar",
        "Mauritius",
        "Mozambique",
        "Namibia",
        "Niger",
        "Nigeria",
        "Republic of the Congo (Congo)",
        "Rwanda",
        "Senegal",
        "South Africa",
        "South Sudan",
        "Sudan",
        "United Republic of Tanzania (Tanzania)",
        "Togo",
        "Uganda",
        "Zambia",
        "Zimbabwe",
        "other African countries and territories",
    ],
}
misspelled = {
    "Democratic People’s Republic of Korea (North Korea)": "PRK",
    "The People’s Republic of China (China)": "CHN",
    "Chinese Taipei": "TWN",
    "Plurinational State of Bolivia (Bolivia)": "BOL",
    "Bolivarian Republic of Venezuela (Venezuela)": "VEN",
    "the Russian Federation (Russia)": "RUS",
    "Kosovo": "XKX",
    "Cyprus,": "CYP",
    "Islamic Republic of Iran (Iran)": "IRN",
    "Syrian Arab Republic (Syria)": "SYR",
    "Lao People’s Democratic Republic (Lao PDR)": "LAO",
    "Timor Leste": "TLS",
    "Côte d’Ivoire": "CIV",
    "Democratic Republic of the Congo (DRC)": "COD",
    "Republic of the Congo (Congo)": "COG",
    "United Republic of Tanzania (Tanzania)": "TZA",
}
pycountry.COUNTRY_NAME.update(misspelled)

#: Pooch information for fetching files.
SOURCE = {  # noqa: E501
    "iea-energy-ai-2025": dict(
        pooch_args=dict(
            base_url="https://iea.blob.core.windows.net/assets/de9dea13-b07d-42c5-a398-d1b3ae17d866/",
            registry={
                "EnergyandAI.pdf": (
                    "3956b80152d4aac6b659a09d0cd7671a4bd53efeeb067e4d50df4230701b33e4"
                ),
            },
        ),
    ),
    "iea-energy-ai-2026": dict(
        pooch_args=dict(
            base_url="https://iea.blob.core.windows.net/assets/3179f7f8-01f6-4dd6-bffa-c9f7b73f1dc9/",
            registry={
                "KeyQuestionsonEnergyandAI.pdf": (
                    "a3a47f07b35420e74b243878f631192b04788110db3c75275feb0fbdee7f4858"
                ),
            },
        ),
    ),
}


def fetch(file_key) -> "Path":
    """Retrieve report for `file_key` from IEA website."""
    from message_ix_models.util import pooch

    # Ensure sources for this module are registered
    pooch.SOURCE.update(SOURCE)

    return pooch.fetch(**pooch.SOURCE[file_key], extra_cache_path="ict")[0]


def read_iea_data() -> pd.DataFrame:
    """Read and format data center electricity demand statistics from IEA documents."""

    # user_agent = (
    #     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 "
    #     "(KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"
    # )
    # ("https://iea.blob.core.windows.net/assets/3179f7f8-01f6-4dd6-bffa-c9f7b73f1dc9/KeyQuestionsonEnergyandAI.pdf")
    p = fetch("iea-energy-ai-2025")
    df2020 = (
        camelot.read_pdf(p, pages="260", flavor="stream", suppress_stdout=True)[0]
        .df.iloc[3:12, :2]
        .set_axis(["Region", 2020], axis=1)
    )
    p = fetch("iea-energy-ai-2026")
    df = (
        camelot.read_pdf(p, pages="110", flavor="stream", suppress_stdout=True)[0]
        .df.iloc[3:12]
        .set_axis(["Region", 2023, 2024, 2025, 2030], axis=1)
    )
    df = df2020.merge(df, how="outer", on="Region").replace(
        {"Central and South America": "Latin America"}
    )
    iea_hist = (
        df.melt(id_vars="Region", var_name="Year", value_name="Value")
        .set_index(["Region", "Year"])
        .astype(float)
    )
    # compute 'Asia Pacific ex China' by subtracting 'China' from 'Asia Pacific'
    asia_pac_ex_china = (
        iea_hist.loc["Asia Pacific"]
        .sub(iea_hist.loc["China"])
        .assign(Region="Asia Pacific ex China")
        .set_index("Region", append=True)
        .swaplevel(0, 1)
    )
    # compute 'Middle East and Africa' by aggregating 'Middle East' & 'Africa'
    afr_mea = (
        iea_hist.loc["Middle East"]
        .add(iea_hist.loc["Africa"])
        .assign(Region="Middle East and Africa")
        .set_index("Region", append=True)
        .swaplevel(0, 1)
    )
    iea_hist = pd.concat([iea_hist, asia_pac_ex_china, afr_mea]).sort_index()
    return iea_hist


def maybe_return_iso(x: str) -> str:
    if pycountry.iso_3166_alpha_3(x):
        return pycountry.iso_3166_alpha_3(x)
    else:
        return x


def get_weo_iso_map() -> dict[str, list[str]]:
    iea_weo_map_flat = {}
    iea_weo_map_child_regs = {
        k for k in iea_weo_map if k in [j for i in iea_weo_map.values() for j in i]
    }
    iea_top_regs = {
        k: v for k, v in iea_weo_map.items() if k not in iea_weo_map_child_regs
    }
    for region, childs in iea_top_regs.items():
        iea_weo_map_flat[region] = []
        for c in childs:
            if c in iea_weo_map_child_regs:
                iea_weo_map_flat[region].extend(iea_weo_map[c])
            else:
                iea_weo_map_flat[region].append(c)
    iso_map = {k: [maybe_return_iso(i) for i in v] for k, v in iea_weo_map_flat.items()}
    iso_map_inv = {i: k for k, v in iso_map.items() for i in v}
    return iso_map_inv


def read_2035_world_projections():
    p = fetch("iea-energy-ai-2025")
    df = camelot.read_pdf(p, pages="258", flavor="stream")[0].df
    return (int(i.replace(" ", "")) for i in df.iloc[23, [11, 7, 5]].values)


def merge_regions(iso_map):
    aggregates = {
        "Europe": ["Europe", "Caspian"],
        "Middle East and Africa": ["Middle East", "Africa"],
    }
    return
