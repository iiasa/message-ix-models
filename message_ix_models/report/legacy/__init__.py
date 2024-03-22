from .report import report as legacy_report  # noqa

ALL_GASES = sorted(
    ["BC", "CH4", "CO2", "N2O", "CO", "NOx", "OC", "Sulfur", "NH3", "VOC"]
)

# default dataframe index
DF_IDX = ["Model", "Scenario", "Region", "Variable", "Unit"]

#  IAMC index
IAMC_IDX = ["Model", "Scenario", "Region", "Variable", "Unit"]
INDEX_ORDER = [
    "Model",
    "Region",
    "Technology",
    "Commodity",
    "Unit",
    "Mode",
    "Grade",
    "Vintage",
]
