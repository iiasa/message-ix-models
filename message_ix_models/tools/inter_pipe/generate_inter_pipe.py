import logging
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

import message_ix
import pandas as pd
import yaml

from message_ix_models import ScenarioInfo
from message_ix_models.util import broadcast, package_data_path

if TYPE_CHECKING:
    from message_ix_models.types import MutableParameterData

log = logging.getLogger(__name__)


@dataclass
class ScenarioConfig:
    start_model: str
    start_scen: str
    target_model: str
    target_scen: str


@dataclass
class SpecConfig:
    """Options for specifying pipeline pairs.

    These allow to use files to filter pipe technologies and regions to a desired set,
    instead of using all combinations.
    """

    #: :any:`True` to use a sheet of all mapped pipe technologies and regions to filter
    #: desired pairs.
    spec_tech_pipe: bool
    #: :any:`True` to use a sheet to specify groups of pipe technologies.
    spec_tech_pipe_group: bool
    #: :any:`True` to use a sheet to specify groups of pipe supply technologies.
    spec_supply_pipe_group: bool


@dataclass
class TechConfig:
    #: Mother commodity name.
    commodity_mother: list[str]

    #: Commodity name suffix.
    commodity_suffix: str
    #: Mother level name.
    level_mother: str
    #: Shortened :attr:`level_mother`.
    level_mother_shorten: str
    #: Level name suffix.
    level_suffix: str
    #: Number of distinct technologies with different investment costs.
    tech_number: int
    #: Mother technology names.
    tech_mother: list[str]
    #: Technology name suffix.
    tech_suffix: str
    #: Shortened :attr:`tech_mother`.
    tech_mother_shorten: Optional[str] = None


@dataclass
class Config:
    #: TODO Document what this means, in contrast to |y0| of the scenario itself.
    first_model_year: int
    #: The pipe technology is the technology that is used to transport the commodity
    #: from one node to another.
    pipe: TechConfig
    #: The pipe supply technology is the technology that feed commodity to the pipe
    #: technology.
    supply: TechConfig
    scenario: ScenarioConfig
    spec: SpecConfig

    @classmethod
    def from_file(cls, path: Union[Path, str, None] = None) -> "Config":
        """Read configuration from file.

        Some notes about the file format:

        - Two top-level keys called ``pipe_tech`` and ``pipe_supplytech`` contain keys
          that have the same structure and names, except for different suffixes. For
          example, ``tech_mother_shorten_pipe`` appears under ``pipe_tech``;
          ``tech_mother_shorten_supply`` appears under ``pipe_supplytech``. This
          function strips the suffixes so both can be stored as instances of
          :class:`TechConfig`.
        """
        full_path = package_data_path("inter_pipe", path or "config").with_suffix(
            ".yaml"
        )
        log.info(f"Load config from {full_path}")
        with open(full_path) as f:
            data = yaml.safe_load(f)

        # Convert to keyword arguments for an instance of Config
        kw = dict(
            first_model_year=data.pop("first_model_year"),
            scenario=ScenarioConfig(**data.pop("scenario")),
            spec=SpecConfig(**data.pop("spec")),
        )
        for yaml_key, name in (("pipe_tech", "pipe"), ("pipe_supplytech", "supply")):
            tech_kw = {k.rpartition("_")[0]: v for k, v in data.pop(yaml_key).items()}
            kw[name] = TechConfig(**tech_kw)

        if len(data):
            log.warning("Ignored config file contents: {data!r}")

        return cls(**kw)


# Small util function
def get_template(scen_base, par_name, tech_mother_pipe):
    template = pd.DataFrame()
    template = (
        scen_base.par(par_name, filters={"technology": tech_mother_pipe})
        .head()
        .iloc[0]
        .to_frame()
        .T
    )
    if template.empty:
        log.warning(
            f"Technology {tech_mother_pipe} does not have {par_name} in {scen_base}."
        )
    return template


# Small util function
def copy_template_columns(df, template, exclude_cols=["node_loc", "technology"]):
    for col in template.columns:
        if col not in exclude_cols:
            df[col] = template[col].iloc[0]


# Main function to generate bare sheets
def inter_pipe_bare(
    base_scen: "message_ix.Scenario",
    # target_model: str,
    # target_scen: str,
    config_name: Union[str, None] = None,
    target_dir: Optional[Path] = None,
):
    """Generate 18 bare sheets to collect minimum data for pipe/supply techs.

    Parmeters
    ---------
    base_scen :
        The base scenario object to start from.
    config_name :
        Name of the config file. If :any:`None`, use default
        :file:`data/inter_pipe/config.yaml`.
    target_dir :
        Directory in which to create files. If :any:`None`, the same
        :file:`data/inter_pipe`.
    """
    # Load the config
    c = config = Config.from_file(config_name)
    config_dir = target_dir or package_data_path("inter_pipe")

    # Use the provided base scenario instead of loading from config
    base = base_scen
    log.info("Using provided base scenario.")

    # Generate export pipe technology: name techs and levels
    set_tech: list[str] = []
    set_level: list[str] = []
    set_relation: list[str] = []
    node_name_base = base.set("node")
    node_name = {
        node
        for node in node_name_base
        if node.lower() != "world" and "glb" not in node.lower()
    }
    if config.spec.spec_tech_pipe is True:
        try:
            spec_tech = pd.read_csv(config_dir.joinpath("spec_tech_pipe.csv"))
            tech_pipe_name = spec_tech["technology"].unique().tolist()
        except FileNotFoundError:
            spec_tech = pd.DataFrame({"node_loc": [], "technology": []})
            spec_tech.to_csv(config_dir / "spec_tech_pipe_edit.csv", index=False)
            raise Exception(
                "The function stopped. Sheet spec_tech_pipe.csv "
                "has been generated. Fill in the specific pairs first and run again."
            )
    else:
        tech_pipe_name = [
            f"{c.pipe.tech_mother_shorten}_{c.pipe.tech_suffix}_exp_{node.split('_')[1]}_{i}"
            for node in node_name
            for i in range(1, config.pipe.tech_number + 1)
        ]
        spec_tech = None

    # Default contents for several files
    df_default = (
        spec_tech
        if spec_tech is not None
        else pd.DataFrame(
            [[n, t] for (n, t) in product(node_name, tech_pipe_name)],
            columns=["node_loc", "technology"],
        )
    )

    # Generate export pipe technology: sheet input_exp_pipe
    template = get_template(base, "input", c.pipe.tech_mother)
    df = df_default.copy()
    # minimum dimensions: nl-m-c-l; required dimensions: nl-t-yv-ya-m-no-c-l-h-ho
    copy_template_columns(df, template)
    df = df.assign(
        year_vtg="broadcast",
        year_act="broadcast",
        mode="M1",
        node_origin=df["node_loc"],
        commodity=c.pipe.commodity_mother,
        level=f"{c.pipe.level_mother_shorten}_{c.pipe.level_suffix}",
        value=None,
    )
    df.to_csv(config_dir / "input_pipe_exp_edit.csv", index=False)
    log.info(f"Input pipe exp csv generated at: {config_dir}.")
    set_tech.extend(df["technology"].unique())
    set_level.extend(df["level"].unique())

    # Generate export pipe technology: sheet output_exp_pipe (no need to edit)
    template = get_template(base, "output", c.pipe.tech_mother)
    df = df_default.copy()
    # minimum dimensions: nl-m-c-l; required dimensions: nl-t-yv-ya-m-no-c-l-h-ho
    copy_template_columns(df, template)
    df = df.assign(
        year_vtg="broadcast",
        year_act="broadcast",
        mode="M1",
        node_dest="R12_GLB",
        commodity=c.pipe.commodity_mother,
        level=df["node_loc"].apply(
            lambda x: f"{c.pipe.level_mother_shorten}_{x.split('_')[1]}"
        ),
        value=1,
    )
    df.to_csv(config_dir / "output_pipe_exp.csv", index=False)
    log.info("Output pipe exp csv generated.")
    set_tech.extend(df["technology"].unique())
    set_level.extend(df["level"].unique())

    # Generate export pipe technology: sheet technical_lifetime_exp_pipe
    template = get_template(base, "technical_lifetime", c.pipe.tech_mother)
    df = df_default.copy()
    # minimum dimensions: nl-t; required dimensions: nl-t-yv
    copy_template_columns(df, template)
    df = df.assign(year_vtg="broadcast", year_act=None, value=None)
    df.to_csv(config_dir / "technical_lifetime_pipe_exp_edit.csv", index=False)
    log.info("Technical lifetime pipe exp csv generated.")
    set_tech.extend(df["technology"].unique())  # set_level.extend(df["level"].unique())

    # Generate export pipe technology: sheet inv_cost_exp_pipe
    template = get_template(base, "inv_cost", c.pipe.tech_mother)
    df = df_default.copy()
    # minimum dimensions: minimum dimensions: nl-t; required dimensions: nl-t-yv
    copy_template_columns(df, template)
    df = df.assign(year_vtg="broadcast", year_act=None, value=None)
    df.to_csv(config_dir / "inv_cost_pipe_exp_edit.csv", index=False)
    log.info("Inv cost pipe exp csv generated.")
    set_tech.extend(df["technology"].unique())  # set_level.extend(df["level"].unique())

    # Generate export pipe technology: sheet fix_cost_exp_pipe
    template = get_template(base, "fix_cost", c.pipe.tech_mother)
    df = df_default.copy()
    # minimum dimensions: minimum dimensions: nl-t; required dimensions: nl-t-yv
    copy_template_columns(df, template)
    df = df.assign(year_vtg="broadcast", year_act="broadcast", value=None)
    df.to_csv(config_dir / "fix_cost_pipe_exp_edit.csv", index=False)
    log.info("Fix cost pipe exp csv generated.")
    set_tech.extend(df["technology"].unique())  # set_level.extend(df["level"].unique())

    # Generate export pipe technology: sheet var_cost_exp_pipe
    template = get_template(base, "var_cost", c.pipe.tech_mother)
    df = df_default.copy()
    # minimum dimensions: minimum dimensions: nl-t; required dimensions: nl-t-yv
    copy_template_columns(df, template)
    df = df.assign(year_vtg="broadcast", year_act="broadcast", value=None)
    df.to_csv(config_dir / "var_cost_pipe_exp_edit.csv", index=False)
    log.info("Var cost pipe exp csv generated.")
    set_tech.extend(df["technology"].unique())  # set_level.extend(df["level"].unique())

    # Generate export pipe technology: sheet capacity_factor_exp_pipe (no need to edit)
    template = get_template(base, "capacity_factor", c.pipe.tech_mother)
    df = df_default.copy()
    # minimum dimensions: nl-t; required dimensions: nl-t-yv-ya-m-h
    copy_template_columns(df, template)
    df = df.assign(year_vtg="broadcast", year_act="broadcast", value=1)
    df.to_csv(config_dir / "capacity_factor_pipe_exp.csv", index=False)
    log.info("Capacity factor pipe exp csv generated.")
    set_tech.extend(df["technology"].unique())  # set_level.extend(df["level"].unique())

    # Generate import pipe technology: name techs and levels
    tech_pipe_name = f"{c.pipe.tech_mother_shorten}_{c.pipe.tech_suffix}_imp"

    # Generate import pipe technology: sheet input_imp_pipe (no need to edit)
    template = get_template(base, "input", c.pipe.tech_mother)
    df = df_default.copy()
    copy_template_columns(df, template)
    df = df.assign(
        technology=tech_pipe_name,
        year_vtg="broadcast",
        year_act="broadcast",
        mode="M1",
        node_origin="R12_GLB",
        commodity=c.pipe.commodity_mother,
        level=df["node_loc"].apply(
            lambda x: f"{c.pipe.level_mother_shorten}_{x.split('_')[1]}"
        ),
        value=1,
    )
    df.to_csv(config_dir / "input_pipe_imp.csv", index=False)
    log.info("Input pipe imp csv generated.")
    set_tech.extend(df["technology"].unique())
    set_level.extend(df["level"].unique())

    # Generate import pipe technology: sheet output_imp_pipe (no need to edit)
    template = get_template(base, "output", c.pipe.tech_mother)
    df = df_default.copy()
    copy_template_columns(df, template)
    df = df.assign(
        technology=tech_pipe_name,
        year_vtg="broadcast",
        year_act="broadcast",
        mode="M1",
        node_dest=df["node_loc"],
        commodity=c.pipe.commodity_mother,
        level=c.pipe.level_mother,
        value=1,
    )
    df.to_csv(config_dir / "output_pipe_imp.csv", index=False)
    log.info("Output pipe imp csv generated.")
    set_tech.extend(df["technology"].unique())
    set_level.extend(df["level"].unique())

    # Generate key relation: pipe -> pipe_group,
    # i.e, grouping exporting pipe technologies
    # If the setting is False, skip processing of relation_tech_group
    # TODO add general function, group all pipe technologies to inter, linking inter to
    #      pipe supply techs
    if config.spec.spec_tech_pipe_group is True:
        try:
            relation_tech_group = pd.read_csv(
                Path(package_data_path("inter_pipe"))
                / "relation_activity_pipe_group.csv"
            )
        except FileNotFoundError:
            spec_tech_group = {
                "relation": ["example_group"],
                "node_rel": ["R12_AFR"],
                "year_rel": ["broadcast"],
                "node_loc": ["R12_AFR"],
                "technology": ["example_tech"],
                "year_act": ["broadcast"],
                "mode": ["M1"],
                "value": [1.0],
                "unit": ["???"],
            }
            relation_tech_group = pd.DataFrame(spec_tech_group)
            relation_tech_group.to_csv(
                config_dir / "relation_activity_pipe_group_edit.csv",
                index=False,
            )
            raise Exception(
                "The function stopped. Sheet relation_activity_pipe_group.csv"
                "has been generated. Fill in the specific pairs first and run again."
            )

        # Only process relation_tech_group if it was defined
        df = relation_tech_group.copy()
        set_tech.extend(df["technology"].unique())
        set_relation.extend(df["relation"].unique())

    # Generate pipe supply technology: name techs and levels
    tech_supply_name = [
        f"{tech}_{c.supply.tech_suffix}" for tech in c.supply.tech_mother
    ]

    # Generate pipe supply technology: sheet output_pipe_supply (no need to edit)
    template = get_template(base, "output", c.supply.tech_mother[0])
    df = pd.DataFrame(
        [[n, t] for (n, t) in product(node_name, tech_supply_name)],
        columns=["node_loc", "technology"],
    )
    copy_template_columns(df, template)
    df = df.assign(
        year_vtg="broadcast",
        year_act="broadcast",
        mode="M1",
        node_dest=df["node_loc"],
        commodity=c.supply.commodity_mother,
        level=f"{c.supply.level_mother_shorten}_{c.supply.level_suffix}",
        value=1,
    )
    df.to_csv(config_dir / "output_pipe_supply.csv", index=False)
    log.info("Output pipe supply csv generated.")
    set_tech.extend(df["technology"].unique())
    set_level.extend(df["level"].unique())
    df.copy()

    def _make_csv(par_name: str, set_value: bool = False) -> None:
        """Generate pipe supply technology sheet for `par_name`."""
        df = base.par(par_name, filters={"technology": c.supply.tech_mother})
        df["technology"] = df["technology"].astype(str) + f"_{c.supply.tech_suffix}"
        if set_value:
            df["value"] = df["value"] * 1  # TODO: debugging
        df.to_csv(config_dir / f"{par_name}_pipe_supply.csv", index=False)
        log.info(f"{par_name} pipe supply csv generated.")

        # Update tracking sets
        set_tech.extend(df["technology"].unique())

    # Generate 5 CSV files; no need to edit
    _make_csv("technical_lifetime")
    _make_csv("inv_cost", set_value=True)
    _make_csv("fix_cost", set_value=True)
    _make_csv("var_cost", set_value=True)
    _make_csv("capacity_factor")

    # Generate key relation: pipe_supply -> pipe,
    # i.e, pipe_supply techs contribute to pipe (group)
    # TODO add general funtion, group all pipe technologies to inter, linking inter to
    #      pipe supply techs
    if config.spec.spec_supply_pipe_group is True:
        try:
            relation_tech_group = pd.read_csv(
                Path(package_data_path("inter_pipe"))
                / "relation_activity_supply_group.csv"
            )
        except FileNotFoundError:
            template_group = {
                "relation": ["example_group"],
                "node_rel": ["R12_AFR"],
                "year_rel": ["broadcast"],
                "node_loc": ["R12_AFR"],
                "technology": ["example_tech"],
                "year_act": ["broadcast"],
                "mode": ["M1"],
                "value": [1.0],
                "unit": ["???"],
            }
            relation_tech_group = pd.DataFrame(template_group)
            relation_tech_group.to_csv(
                config_dir / "relation_activity_supply_group_edit.csv",
                index=False,
            )
            raise Exception(
                "The function stopped."
                "Sheet relation_activity_supply_group.csv has been generated. "
                "Fill in the specific pairs first and run again."
            )

        # Only process relation_tech_group if it was defined
        df = relation_tech_group.copy()
        set_tech.extend(df["technology"].unique())
        set_relation.extend(df["relation"].unique())

    # Generate technology set sheet (no need to edit)
    technology = list(set(set_tech))
    df = pd.DataFrame({"technology": technology})
    df.to_csv(config_dir / "technology.csv", index=False)
    log.info("Set technology csv generated.")

    # Generate commodity set sheet (no need to edit)

    # Generate level set sheet (no need to edit)
    level = list(set(set_level))
    df = pd.DataFrame({"level": level})
    df.to_csv(config_dir / "level.csv", index=False)
    log.info("Set level csv generated.")

    # Generate relation set sheet (no need to edit)
    relation = list(set(set_relation))
    # Hard-coded for optioal relation filled by addtional input files
    # TODO: put hard-coded relations in config too
    hard_coded_relation = ["elec_share_gei", "elec_share_gei_CHN", "elec_share_gei_FSU"]
    for rel in hard_coded_relation:
        if rel not in relation:
            relation.append(rel)
    df = pd.DataFrame({"relation": relation})
    df.to_csv(config_dir / "relation.csv", index=False)
    log.info("Set relation csv generated.")

    # # Keep track of all csv files
    # csv_files = []
    # csv_files.append(os.path.join(config_dir, "input_pipe_exp.csv"))
    # #TODO: might be nice to have a csv list

    # return csv_files


def inter_pipe_build(
    scen: "message_ix.Scenario",
    config_name: Optional[str] = None,
    data_dir: Optional[Path] = None,
) -> "message_ix.Scenario":
    """Read the input csv files and build the pipe tech sets and parameters.

    Parameters
    ----------
    scen :
        The target scenario object to build inter_pipe on.
    config_name :
        Name of the config file. If :any:`None`, use default
        :file:`data/inter_pipe/config.yaml`.
    data_dir :
        Directory in which to locate CSV data files. See :func:`read_data`.
    """
    # Load the data
    data_dict = read_data(data_dir)
    # Load the config
    config = Config.from_file(config_name)

    # Generate par_list and set_list
    par_list = [
        "input",
        "output",
        "technical_lifetime",
        "inv_cost",
        "fix_cost",
        "var_cost",
        "capacity_factor",
        "relation_activity",
        "relation_upper",
        "relation_lower",
    ]
    set_list = [
        "technology",
        # "commodity",
        "level",
        "relation",
    ]

    # Information about the target scenario
    info = ScenarioInfo(scen)
    if info.y0 != config.first_model_year:
        raise NotImplementedError(
            f"inter_pipe_build() with y₀ = {info.y0} != {config.first_model_year}"
        )

    # Broadcast the data
    # TODO This could be further simplified:
    #      - Definte a function like _cols_to_broadcast() that returns year_* column
    #        names containing 'broadcast'
    #      - Switch on the return value of this function instead of nested if-blocks.
    for i in data_dict.keys():
        if (
            "year_rel" in data_dict[i].columns
            and data_dict[i]["year_rel"].iloc[0] == "broadcast"
        ):
            data_dict[i] = (
                data_dict[i]
                .replace("broadcast", None)
                .pipe(broadcast, year_rel=info.Y)
                .assign(year_act=lambda df: df.year_rel)
            )
            log.info(f"Parameter {i} Broadcasted.")
        elif "year_vtg" in data_dict[i].columns and "year_act" in data_dict[i].columns:
            if (
                data_dict[i]["year_vtg"].iloc[0] == "broadcast"
                and data_dict[i]["year_act"].iloc[0] == "broadcast"
            ):
                data_dict[i] = (
                    data_dict[i]
                    .replace("broadcast", None)
                    .pipe(broadcast, info.yv_ya.query("year_vtg >= @info.y0"))
                )
                log.info(f"Parameter {i} Broadcasted.")
            elif (
                data_dict[i]["year_vtg"].iloc[0] == "broadcast"
                and data_dict[i]["year_act"].iloc[0] != "broadcast"
            ):
                data_dict[i] = (
                    data_dict[i]
                    .replace("broadcast", None)
                    .pipe(broadcast, year_vtg=info.Y)
                )
                log.info(f"Parameter {i} Broadcasted.")

    # Generate relation upper and lower
    for i in filter(
        lambda k: "relation_activity" in k and k != "relation_activity_ori", data_dict
    ):
        key_name = i
        df = data_dict[i][["relation", "node_rel", "year_rel", "unit"]].assign(value=0)
        df = df.pipe(broadcast, year_rel=info.Y)
        key_name_upper = key_name.replace("activity", "upper")
        data_dict[key_name_upper] = df.copy()

        key_name_lower = key_name.replace("activity", "lower")
        data_dict[key_name_lower] = df.copy()

    # Add set and parameter
    with scen.transact("Added"):
        for i in filter(data_dict.__contains__, set_list):
            i_str = (
                data_dict[i]
                .apply(lambda row: row.astype(str).str.cat(sep=", "), axis=1)
                .tolist()
            )  # str or list of str only
            scen.add_set(i, i_str)
            log.info(f"Set {i} added.")
        for i in par_list:
            # Find all keys in data_dict that contain the parameter name
            matching_keys = [k for k in data_dict.keys() if i in k]
            if matching_keys:
                # Combine all matching DataFrames
                combined_df = pd.concat(
                    [data_dict[k] for k in matching_keys], ignore_index=True
                )
                scen.add_par(i, combined_df)
                log.info(f"Parameter {i} from {matching_keys} added.")

    return scen


def read_data(base_dir: Optional[Path] = None) -> "MutableParameterData":
    """Read the :mod:`inter_pipe` data files.

    Files with "edit" in the name are generated by :func:`inter_pipe_bare` and are
    assumed to need editing, so are ignored.

    Parameters
    ----------
    base_dir :
        Directory in which to locate CSV data files. If not given, the package data
        directory :file:`data/inter_pipe/` is used.
    """
    base_dir = base_dir or package_data_path("inter_pipe")

    result = {}
    for p in base_dir.glob("*.csv"):
        if "edit" in p.name:
            log.info(f"Ignore {p} containing 'edit'.")
            continue
        result[p.stem] = pd.read_csv(p)
    return result
