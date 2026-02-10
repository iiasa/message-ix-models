from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Literal

import message_ix
import pandas as pd
from ixmp import Platform
from message_ix import make_df
from pandas import DataFrame

from message_ix_models import ScenarioInfo
from message_ix_models.util import (
    broadcast,
    nodes_ex_world,
    package_data_path,
    same_node,
)

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models.types import ParameterData


@dataclass
class CommShareConfig:
    """Configuration for commodity share constraints.

    Collects and prepares all data required to add a new commodity share constraint to
    the model structure of a ``Scenario``.

    ``mode-commodity-level`` indices for ``map_shares_commodity_<total/share>`` can be
    inferred from ``input`` parameter data from a scenario based on the provided
    technology lists ``all_tecs`` and ``share_tecs``.

    Alternatively, they can be defined explicitly through ``dims_all`` and
    ``dims_share`` in the YAML config file.

    The same applies to ``nodes``. If ``nodes: all``, all model regions except "World"
    are used.
    """

    share_name: str
    type_tec_all_name: str
    type_tec_share_name: str
    all_tecs: List[str]
    share_tecs: List[str]
    nodes: str | List[str]
    dims_all: dict[str, List[str]] = field(default_factory=dict[str, List[str]])
    dims_share: dict[str, List[str]] = field(default_factory=dict[str, List[str]])

    @classmethod
    def from_files(cls, scen: "Scenario", name: str) -> "CommShareConfig":
        """Create a CommShareConfig instance from YAML file and model structure."""
        import yaml

        # Handle basic configuration file
        path = package_data_path("material", "share_constraints.yaml")
        with open(path) as f:  # Raises FileNotFoundError on missing file
            kw = yaml.safe_load(f)[name]  # Raises on invalid YAML
        # Create a CommShareConfig instance
        result = cls(**kw)
        result.nodes = (
            nodes_ex_world(ScenarioInfo(scen).N)
            if result.nodes == "all"
            else result.nodes
        )
        if not result.dims_all:
            result.dims_all = result.dims_from_tecs(scen, "all")
        if not result.dims_share:
            result.dims_share = result.dims_from_tecs(scen, "share")
        return result

    def dims_from_tecs(
        self, scen: "Scenario", tec_type: Literal["all", "share"]
    ) -> dict[str, List[str]]:
        """Initialize ``dims_all`` or ``dims_share`` from model structure."""
        tec_map = {"all": self.all_tecs, "share": self.share_tecs}
        dims = (
            scen.par("input", filters={"technology": tec_map[tec_type]})[
                ["mode", "commodity", "level"]
            ]
            .drop_duplicates()
            .to_dict("records")
        )
        merged: dict[str, List[str]] = {
            k: [d[k] for d in dims if k in d] for k in {k for d in dims for k in d}
        }
        return merged

    def get_map_share_set_total(self, scen: "Scenario") -> pd.DataFrame:
        """Generate ``map_shares_commodity_total`` from config and model structure."""
        return (
            make_df(
                "map_shares_commodity_total",
                shares=self.share_name,
                type_tec=self.type_tec_all_name,
                **self.dims_from_tecs(scen, "all"),
            )
            .pipe(broadcast, node=self.nodes)
            .pipe(same_node, from_col="node")
        )

    def get_map_share_set_share(self, scen) -> pd.DataFrame:
        """Generate ``map_shares_commodity_share`` from config and model structure."""
        return (
            make_df(
                "map_shares_commodity_share",
                shares=self.share_name,
                type_tec=self.type_tec_share_name,
                **self.dims_from_tecs(scen, "share"),
            )
            .pipe(broadcast, node=self.nodes)
            .pipe(same_node, from_col="node")
        )

    def add_to_scenario(self, scen: "Scenario") -> None:
        """Add all required sets and categories to a scenario."""
        scen.add_set("shares", self.share_name)
        scen.add_cat("technology", self.type_tec_all_name, self.all_tecs)
        scen.add_cat("technology", self.type_tec_share_name, self.share_tecs)
        scen.add_set("map_shares_commodity_share", self.get_map_share_set_share(scen))
        scen.add_set("map_shares_commodity_total", self.get_map_share_set_total(scen))

    def remove_from_scenario(self, scen) -> None:
        """Remove all required sets and categories to a scenario."""
        scen.remove_set("shares", self.share_name)
        scen.remove_set("type_tec", self.type_tec_all_name)
        scen.remove_set("type_tec", self.type_tec_share_name)
        scen.remove_set(
            "map_shares_commodity_share", self.get_map_share_set_share(scen)
        )
        scen.remove_set(
            "map_shares_commodity_total", self.get_map_share_set_total(scen)
        )


def gen_com_share_df(
    shr_name: str, df_vals: pd.DataFrame, type: Literal["up", "lo"] = "up"
) -> pd.DataFrame:
    """Generate DataFrame for ``share_commodity_up/lo`` parameter.

    Parameters
    ----------
    shr_name
        name of the share constraint
    df_vals
        DataFrame with columns ["node", "Value"]
    type
        "lo" for minimum constraint and "up" for maximum constraint

    Returns
    -------
    pd.DataFrame
        ``share_commodity_up/lo`` parameter data
    -------
    """
    df_share_com = make_df(f"share_commodity_{type}", **df_vals).assign(
        shares=shr_name, time="year", unit="-"
    )
    return df_share_com


def gen_comm_shr_par(
    scen: "Scenario",
    cname: str,
    shr_vals_df: pd.DataFrame,
    shr_type: Literal["up", "lo"] = "up",
    years: str | List[int] = "all",
) -> DataFrame:
    """Generates data frame for "share_commodity_up/lo" parameter with given values for
    node_share and broadcasts them for given "years".

    Parameters
    ----------
    scen
        used if years == "all" to obtain model years for column "year"
    cname
        code to use for "share" column
    shr_vals_df
        data frame with columns ["node_share", "value"]
    shr_type
        "up" or "lo"
    years
        "all" to generate for all optimization years of scen or list of years
    """
    req_cols = ["node_share", "value"]
    check_cols = any(item in shr_vals_df for item in req_cols)
    if not check_cols:
        raise ValueError(f"shr_vals_df does not have the columns {req_cols}")
    df_final = shr_vals_df.copy(deep=True)
    if isinstance(years, str):
        if years == "all":
            years = scen.yv_ya()["year_act"].drop_duplicates()
    if "year_act" not in shr_vals_df.columns:
        df_final["year_act"] = None
        df_final = df_final.pipe(broadcast, year_act=years)

    df_shares: pd.DataFrame = gen_com_share_df(cname, df_final, shr_type)
    return df_shares


def add_comm_share(
    scen: "Scenario",
    name: str,
    shr_vals: pd.DataFrame,
    shr_type: Literal["up", "lo"] = "up",
    years: str | List[int] = "all",
):
    return gen_comm_shr_par(scen, name, shr_vals, shr_type=shr_type, years=years)


def add_foil_shr_constraint() -> None:
    """Generate fuel oil share constraint for MESSAGEix-Materials industry sectors.

    ** Not used in model build at the moment. **
    """
    # shr_const = "share_low_lim_foil_ind"
    # type_tec_shr = "foil_cement"
    # type_tec_tot = "all_cement"

    from share_constraints_constants import foil_ind_tecs_ht, non_foil_ind_tecs_ht

    df_furn_cement = pd.read_csv(
        r"C:/Users\maczek\PycharmProjects\IEA_activity_calib_SSP/furnace_foil_cement_share.csv",
        usecols=[0, 2],
    )
    df_furn_cement = df_furn_cement.set_axis(["node_share", "value"], axis=1)
    all_ind_tecs = {
        a[0]: [a[1], *b[1]]
        for a, b in zip(foil_ind_tecs_ht.items(), non_foil_ind_tecs_ht.items())
    }

    foil_sectors = set(all_ind_tecs.keys())
    foil_sectors.remove("resins")

    model = ""
    scenario = ""
    mp = Platform()
    scen = message_ix.Scenario(mp, model, scenario)
    sc_clone = scen.clone(scen.model, scen.scenario + "foil_furn", keep_solution=False)

    for sec in foil_sectors:
        # df_sec_foil_shr = pd.read_csv(
        # f"C:/Users\maczek\PycharmProjects\IEA_activity_calib_SSP/
        # furnace_foil_{sec}_share.csv",
        #     usecols=[0, 2],
        # )
        df_furn_cement = df_furn_cement.set_axis(["node_share", "value"], axis=1)
        add_comm_share(
            sc_clone,
            f"{sec}_foil",
            df_furn_cement,
            years=sc_clone.yv_ya()["year_act"].drop_duplicates()[1:],
        )


def add_industry_coal_shr_constraint(scen: "Scenario") -> "ParameterData":
    """Add an upper share constraint for coal use in residual industry sector."""
    name = "UE_industry_th_coal"
    share_reg_values = pd.read_csv(
        package_data_path("material", "other", "coal_i_shares_2020.csv")
    )
    par_data = add_comm_share(
        scen,
        name,
        share_reg_values,
        "up",
        years=scen.yv_ya()["year_act"].drop_duplicates()[2:],
    )
    return {"share_commodity_up": par_data}
