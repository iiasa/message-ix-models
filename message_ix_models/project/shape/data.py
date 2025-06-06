"""Handle data from the SHAPE project."""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from message_ix_models.tools.exo_data import BaseOptions, ExoDataSource, register_source
from message_ix_models.tools.iamc import iamc_like_data_for_query
from message_ix_models.util import path_fallback

if TYPE_CHECKING:
    from typing import NotRequired, TypedDict

    from genno.types import AnyQuantity

    Info = TypedDict(
        "Info",
        {"latest": str, "suffix": str, "variable": str, "drop": NotRequired[list[str]]},
    )

log = logging.getLogger(__name__)

#: Information about data file version, suffixes, "variable" codes, and extra columns to
#: drop.
INFO: dict[str, "Info"] = {
    "gdp": dict(
        latest="1.2",
        suffix=".mif",
        variable="GDP|PPP",
    ),
    "gini": dict(
        drop=[
            "tgt.achieved",
            "Base gini imputed",
            "Share of final consumption among GDP imputed",
        ],
        latest="1.1",
        suffix=".csv",
        variable="Gini",
    ),
    "population": dict(
        latest="1.2",
        suffix=".mif",
        variable="Population",
    ),
    "urbanisation": dict(
        drop=["Notes"],
        latest="1.0",
        suffix=".csv",
        variable="Population|Urban|Share",
    ),
}

#: Convert unit forms appearing in files to pint-compatible expressions.
UNITS = {
    "%": "",  # urbanisation
    "billion $2005/yr": "GUSD_2005 / year",  # gdp
    "NA": "dimensionless",  # gini
}


@register_source
class SHAPE(ExoDataSource):
    """Provider of exogenous data from the SHAPE project data source."""

    @dataclass
    class Options(BaseOptions):
        #: Must be one of the keys of :data:`.INFO`.
        measure: str = ""

        #: Version of the data, either "latest" or a string like "1.2".
        version: str = "latest"

        #: One of the SHAPE "SDP" scenario names.
        scenario: str = ""

    options: Options

    where = ["private"]

    def __init__(self, *args, **kwargs) -> None:
        opt = self.options = self.Options.from_args(self, *args, **kwargs)
        try:
            # Retrieve information about the `quantity`
            info = INFO[opt.measure]
        except KeyError:
            raise ValueError(f"measure must be one of {sorted(INFO.keys())}")

        # Choose the version: replace "latest" with the actual version
        version = opt.version.replace("latest", info["latest"])

        # Construct path to data file
        filename = f"{opt.measure}_v{version.replace('.', 'p')}{info['suffix']}"
        self.path = path_fallback("shape", filename, where=self._where())

        # Query for iamc_like_data_for_query()
        variable = info.get("variable", opt.measure)
        self.query = (
            f"Scenario == {opt.scenario!r}" if opt.scenario else "True"
        ) + f" and Variable == {variable!r}"

        self.to_drop = info.get("drop", [])
        self.unique = "MODEL VARIABLE UNIT"
        if opt.scenario:
            # Require a unique scenario
            self.unique += " SCENARIO"
        else:
            # Result will have a "SCENARIO" dimension
            self.options.dims += ("SCENARIO",)

        # Create .key
        super().__init__()

    def get(self) -> "AnyQuantity":
        """Load the data.

        1. Read the file. Use ";" for .mif files; set columns as index on load.
        2. Drop columns "Model" (meaningless); others from `info`.
        3. Drop empty columns (final column in .mif files).
        4. Convert column labels to integer.
        5. Stack to long format.
        6. Apply final column names.
        """
        return iamc_like_data_for_query(
            self.path,
            self.query,
            drop=self.to_drop,
            replace={"Unit": UNITS},
            unique=self.unique,
            # For pd.DataFrame.read_csv()
            na_values=[""],
            keep_default_na=False,
            sep=";" if self.path.suffix == ".mif" else ",",
        )
