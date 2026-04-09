import tabula
import pandas as pd
from message_ix_models.model.material.util import add_region_column
from message_ix_models.util import pycountry

# custom dictionary as fallback lookup table for country names that are not recognized by pycountry
# it also contains some country names that were only used in older yearbooks
iso_dict = {
    "Belgium-Luxembourg": "BEL",
    "Bosnia-Herzegovina": "BIH",
    "Cuba": "CUB",
    "Dem. Rep. of the Congo": "COD",
    "D.P.R. Korea": "PRK",
    "D.R. Congo": "COD",
    "Hong Kong, China": "HKG",
    "Ivory Coast": "CIV",
    "Macedonia": "MKD",
    "Other C.I.S.": "ARM",
    "Russia": "RUS",
    "Serbia and Montenegro": "SRB",
    "Slovakia (e:2018-20)": "SVK",
    "Taiwan, China": "TWN",
    "Turkey": "TUR",
}


def read_pdf_pages(file_path: str, pages: int | list[int]):
    dfs = tabula.read_pdf(file_path, pages=pages, multiple_tables=False)
    df = dfs[0].rename(columns={"Unnamed: 0": "Country"})
    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col].str.replace(" ", ""), errors="coerce")
    replace_country_footnotes(df)
    return df


def replace_country_footnotes(df: pd.DataFrame):
    df["Country"] = (
        df["Country"]
        .str.replace(" (1)", "")
        .str.replace(" (2)", "")
        .str.replace(" (3)", "")
        .str.replace(" (e)", "")
    )


def gen_r12_data(df: pd.DataFrame):
    cols = [i for i in df.columns if i.isdigit()]
    pycountry.COUNTRY_NAME.update(iso_dict)
    df["ISO"] = df.reset_index()["Country"].apply(
        lambda x: pycountry.iso_3166_alpha_3(x)
    )
    df[df['Country'].isin(['European Union (27)', 
                           'o/w: extra-E.U. (27)',
                           'North America',
                           'Other Africa', 'Other Europe', 'Other Middle East', 'Other North America', 'Other South America',
                           'Russia & other CIS + Ukraine', 'World']) == False]
    df["R12"] = add_region_column(df, ("node", "R12.yaml"), iso_column="ISO")
    return df
    #return df.groupby("R12").sum()[cols]


def get_net_trade(file_path: str, exp_pages: int | list[int], imp_pages: int| list[int]):
    df_exp = read_pdf_pages(file_path, exp_pages)
    df_imp = read_pdf_pages(file_path, imp_pages)
    cols = [i for i in df_exp.columns if i.isdigit()]

    df_trade = (
        df_imp.set_index("Country")[cols]
        .sub(df_exp.set_index("Country")[cols], fill_value=0)
        .reset_index()
    )
    return df_trade
