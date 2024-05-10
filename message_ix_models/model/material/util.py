import os

import openpyxl as pxl
import pandas as pd
import yaml
from scipy.optimize import curve_fit

from message_ix_models import Context
from message_ix_models.util import load_private_data, private_data_path

# Configuration files
METADATA = [
    # ("material", "config"),
    ("material", "set"),
    # ("material", "technology"),
]


def read_config():
    """Read configuration from set.yaml."""
    # TODO this is similar to transport.utils.read_config; make a common
    #      function so it doesn't need to be in this file.
    context = Context.get_instance(-1)

    if "material set" in context:
        # Already loaded
        return context

    # Load material configuration
    for parts in METADATA:
        # Key for storing in the context
        key = " ".join(parts)

        # Actual filename parts; ends with YAML
        _parts = list(parts)
        _parts[-1] += ".yaml"

        context[key] = load_private_data(*_parts)

    # Read material.yaml
    # context.metadata_path=Path("C:/Users/unlu/Documents/GitHub/message_data/data")
    # context.load_config("material", "set")

    # Use a shorter name
    context["material"] = context["material set"]

    # Merge technology.yaml with set.yaml
    # context["material"]["steel"]["technology"]["add"] = (
    #     context.pop("transport technology")
    # )

    return context


def prepare_xlsx_for_explorer(filepath):
    df = pd.read_excel(filepath)

    def add_R12(str):
        if len(str) < 5:
            return "R12_" + str
        else:
            return str

    df = df[~df["Region"].isna()]
    df["Region"] = df["Region"].map(add_R12)
    df.to_excel(filepath, index=False)


def combine_df_dictionaries(*args):
    keys = set([key for tup in args for key in tup])
    comb_dict = {}
    for i in keys:
        comb_dict[i] = pd.concat([j.get(i) for j in args])
    return comb_dict


def read_yaml_file(file_path):
    with open(file_path, encoding="utf8") as file:
        try:
            data = yaml.safe_load(file)
            return data
        except yaml.YAMLError as e:
            print(f"Error while parsing YAML file: {e}")
            return None


def invert_dictionary(original_dict):
    inverted_dict = {}
    for key, value in original_dict.items():
        for array_element in value:
            if array_element not in inverted_dict:
                inverted_dict[array_element] = []
            inverted_dict[array_element].append(key)
    return inverted_dict


def excel_to_csv(material_dir, fname):
    xlsx_dict = pd.read_excel(
        private_data_path("material", material_dir, fname), sheet_name=None
    )
    if not os.path.isdir(private_data_path("material", "version control")):
        os.mkdir(private_data_path("material", "version control"))
    os.mkdir(private_data_path("material", "version control", fname))
    for tab in xlsx_dict.keys():
        xlsx_dict[tab].to_csv(
            private_data_path("material", "version control", fname, f"{tab}.csv"),
            index=False,
        )


def get_all_input_data_dirs():
    elements = os.listdir(private_data_path("material"))
    elements = [i for i in elements if os.path.isdir(private_data_path("material", i))]
    return elements


def remove_from_list_if_exists(element, _list):
    if element in _list:
        _list.remove(element)


def exponential(x, b, m):
    return b * m ** x


def price_fit(df):
    # print(df.lvl)
    pars, cov = curve_fit(exponential, df.year, df.lvl, maxfev=5000)
    val = exponential([2020], *pars)[0]
    # print(df.commodity.unique(), df.node.unique(), val)
    return val


def cost_fit(df):
    # print(df.lvl)
    pars, cov = curve_fit(exponential, df.year, df.lvl, maxfev=5000)
    val = exponential([2020], *pars)[0]
    # print(df.node.unique(), val)
    return val / 1000


def update_macro_calib_file(scenario, fname):
    path = "C:/Users/maczek/Downloads/macro/refactored/"
    wb = pxl.load_workbook(path + fname)

    # cost_ref
    years = [i for i in range(2020, 2055, 5)]
    df = scenario.var("COST_NODAL_NET", filters={"year": years})
    df["node"] = pd.Categorical(
        df["node"],
        [
            "R12_AFR",
            "R12_CHN",
            "R12_EEU",
            "R12_FSU",
            "R12_LAM",
            "R12_MEA",
            "R12_NAM",
            "R12_PAO",
            "R12_PAS",
            "R12_RCPA",
            "R12_SAS",
            "R12_WEU",
        ],
    )
    df = df[df["year"].isin([2025, 2030, 2035])].groupby(["node"]).apply(cost_fit)
    ws = wb.get_sheet_by_name("cost_ref")
    for i in range(2, 7):
        ws[f"B{i}"].value = df.values[i - 2]
    for i in range(8, 13):
        ws[f"B{i}"].value = df.values[i - 2]

    # price_ref
    comms = ["i_feed", "i_spec", "i_therm", "rc_spec", "rc_therm", "transport"]
    years = [i for i in range(2025, 2055, 5)]
    df = scenario.var("PRICE_COMMODITY", filters={"commodity": comms, "year": years})
    df["node"] = pd.Categorical(
        df["node"],
        [
            "R12_AFR",
            "R12_EEU",
            "R12_FSU",
            "R12_LAM",
            "R12_MEA",
            "R12_NAM",
            "R12_PAO",
            "R12_PAS",
            "R12_SAS",
            "R12_WEU",
            "R12_CHN",
            "R12_RCPA",
        ],
    )
    df["commodity"] = pd.Categorical(
        df["commodity"],
        ["i_feed", "i_spec", "i_therm", "rc_spec", "rc_therm", "transport"],
    )
    df = df.groupby(["node", "commodity"]).apply(price_fit)
    ws = wb.get_sheet_by_name("price_ref")
    for i in range(2, 61):
        ws[f"C{i}"].value = df.values[i - 2]
    wb.save(path + fname)
