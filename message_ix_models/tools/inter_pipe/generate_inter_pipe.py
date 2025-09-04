import logging
from pathlib import Path
from typing import Optional, Union

import message_ix
import pandas as pd
import yaml

from message_ix_models import ScenarioInfo
from message_ix_models.util import broadcast, package_data_path

log = logging.getLogger(__name__)


def load_config(full_path):
    with open(full_path, "r") as f:
        config = yaml.safe_load(f)
        # safe_load is recommended over load for security
    return config


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
# ruff: noqa: C901
def inter_pipe_bare(
    base_scen: "message_ix.Scenario",
    # target_model: str,
    # target_scen: str,
    config_name: Union[str, None] = None,
):
    """
    Generate bare sheets to collect (minimum) parameters for pipe
    technologies and pipe supply technologies.

    Args:
        base_scen: The base scenario object to start from
        config_name (str, optional): Name of the config file.
            If None, uses default config from data/inter_pipe/config.yaml
    """
    # Load the config
    if config_name is None:
        config_name = "config.yaml"
    full_path = package_data_path("inter_pipe", config_name)
    config_dir = full_path.parent
    config = load_config(full_path)
    log.info(f"Loading config from: {full_path}")

    # Get all config sections
    pipe_tech_config = config.get("pipe_tech", {})
    pipe_supplytech_config = config.get("pipe_supplytech", {})
    tech_mother_pipe, tech_shorten_mother_pipe, tech_suffix_pipe, tech_number_pipe = (
        pipe_tech_config.get("tech_mother_pipe", []),
        pipe_tech_config.get("tech_shorten_mother_pipe", []),
        pipe_tech_config.get("tech_suffix_pipe", []),
        pipe_tech_config.get("tech_number_pipe", []),
    )
    commodity_mother_pipe, _commodity_suffix_pipe = (
        pipe_tech_config.get("commodity_mother_pipe", []),
        pipe_tech_config.get("commodity_suffix_pipe", []),
    )
    level_mother_pipe, level_shorten_mother_pipe, level_suffix_pipe = (
        pipe_tech_config.get("level_mother_pipe", []),
        pipe_tech_config.get("level_shorten_mother_pipe", []),
        pipe_tech_config.get("level_suffix_pipe", []),
    )
    tech_mother_supply, tech_suffix_supply, _tech_number_supply = (
        pipe_supplytech_config.get("tech_mother_supply", []),
        pipe_supplytech_config.get("tech_suffix_supply", []),
        pipe_supplytech_config.get("tech_number_supply", []),
    )
    commodity_mother_supply, _commodity_suffix_supply = (
        pipe_supplytech_config.get("commodity_mother_supply", []),
        pipe_supplytech_config.get("commodity_suffix_supply", []),
    )
    _level_mother_supply, level_mother_shorten_supply, level_suffix_supply = (
        pipe_supplytech_config.get("level_mother_supply", []),
        pipe_supplytech_config.get("level_mother_shorten_supply", []),
        pipe_supplytech_config.get("level_suffix_supply", []),
    )

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
    spec_tech_pipe = config.get("spec", {}).get("spec_tech_pipe", True)
    if spec_tech_pipe is True:
        try:
            spec_tech = pd.read_csv(
                Path(package_data_path("inter_pipe")) / "spec_tech_pipe.csv"
            )
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
            f"{tech_shorten_mother_pipe[0]}_{tech_suffix_pipe[0]}_exp_{node.split('_')[1]}_{i}"
            for node in node_name
            for i in range(1, tech_number_pipe[0] + 1)
        ]
        spec_tech = None

    # Generate export pipe technology: sheet input_exp_pipe
    template = get_template(base, "input", tech_mother_pipe)
    if spec_tech is not None:
        df = spec_tech
    else:
        df = pd.DataFrame(
            {
                "node_loc": [node for node in node_name for _ in tech_pipe_name],
                "technology": [tech for _ in node_name for tech in tech_pipe_name],
            }
        )  # minimum dimensions: nl-m-c-l; required dimensions: nl-t-yv-ya-m-no-c-l-h-ho
    copy_template_columns(df, template)
    df = df.assign(
        year_vtg="broadcast",
        year_act="broadcast",
        mode="M1",
        node_origin=df["node_loc"],
        commodity=commodity_mother_pipe[0],
        level=f"{level_shorten_mother_pipe[0]}_{level_suffix_pipe[0]}",
        value=None,
    )
    df.to_csv(config_dir / "input_pipe_exp_edit.csv", index=False)
    log.info(f"Input pipe exp csv generated at: {config_dir}.")
    set_tech.extend(df["technology"].unique())
    set_level.extend(df["level"].unique())
    df.copy()

    # Generate export pipe technology: sheet output_exp_pipe (no need to edit)
    template = get_template(base, "output", tech_mother_pipe)
    if spec_tech is not None:
        df = spec_tech
    else:
        df = pd.DataFrame(
            {
                "node_loc": [node for node in node_name for _ in tech_pipe_name],
                "technology": [tech for _ in node_name for tech in tech_pipe_name],
            }
        )  # minimum dimensions: nl-m-c-l; required dimensions: nl-t-yv-ya-m-no-c-l-h-ho
    copy_template_columns(df, template)
    df = df.assign(
        year_vtg="broadcast",
        year_act="broadcast",
        mode="M1",
        node_dest="R12_GLB",
        commodity=commodity_mother_pipe[0],
        level=df["node_loc"].apply(
            lambda x: f"{level_shorten_mother_pipe[0]}_{x.split('_')[1]}"
        ),
        value=1,
    )
    df.to_csv(config_dir / "output_pipe_exp.csv", index=False)
    log.info("Output pipe exp csv generated.")
    set_tech.extend(df["technology"].unique())
    set_level.extend(df["level"].unique())
    df.copy()

    # Generate export pipe technology: sheet technical_lifetime_exp_pipe
    template = get_template(base, "technical_lifetime", tech_mother_pipe)
    if spec_tech is not None:
        df = spec_tech
    else:
        df = pd.DataFrame(
            {
                "node_loc": [node for node in node_name for _ in tech_pipe_name],
                "technology": [tech for _ in node_name for tech in tech_pipe_name],
            }
        )  # minimum dimensions: nl-t; required dimensions: nl-t-yv
    copy_template_columns(df, template)
    df = df.assign(year_vtg="broadcast", year_act=None, value=None)
    df.to_csv(config_dir / "technical_lifetime_pipe_exp_edit.csv", index=False)
    log.info("Technical lifetime pipe exp csv generated.")
    set_tech.extend(df["technology"].unique())  # set_level.extend(df["level"].unique())
    df.copy()

    # Generate export pipe technology: sheet inv_cost_exp_pipe
    template = get_template(base, "inv_cost", tech_mother_pipe)
    if spec_tech is not None:
        df = spec_tech
    else:
        df = pd.DataFrame(
            {
                "node_loc": [node for node in node_name for _ in tech_pipe_name],
                "technology": [tech for _ in node_name for tech in tech_pipe_name],
            }
        )  # minimum dimensions: minimum dimensions: nl-t; required dimensions: nl-t-yv
    copy_template_columns(df, template)
    df = df.assign(year_vtg="broadcast", year_act=None, value=None)
    df.to_csv(config_dir / "inv_cost_pipe_exp_edit.csv", index=False)
    log.info("Inv cost pipe exp csv generated.")
    set_tech.extend(df["technology"].unique())  # set_level.extend(df["level"].unique())
    df.copy()

    # Generate export pipe technology: sheet fix_cost_exp_pipe
    template = get_template(base, "fix_cost", tech_mother_pipe)
    if spec_tech is not None:
        df = spec_tech
    else:
        df = pd.DataFrame(
            {
                "node_loc": [node for node in node_name for _ in tech_pipe_name],
                "technology": [tech for _ in node_name for tech in tech_pipe_name],
            }
        )  # minimum dimensions: minimum dimensions: nl-t; required dimensions: nl-t-yv
    copy_template_columns(df, template)
    df = df.assign(year_vtg="broadcast", year_act="broadcast", value=None)
    df.to_csv(config_dir / "fix_cost_pipe_exp_edit.csv", index=False)
    log.info("Fix cost pipe exp csv generated.")
    set_tech.extend(df["technology"].unique())  # set_level.extend(df["level"].unique())
    df.copy()

    # Generate export pipe technology: sheet var_cost_exp_pipe
    template = get_template(base, "var_cost", tech_mother_pipe)
    if spec_tech is not None:
        df = spec_tech
    else:
        df = pd.DataFrame(
            {
                "node_loc": [node for node in node_name for _ in tech_pipe_name],
                "technology": [tech for _ in node_name for tech in tech_pipe_name],
            }
        )  # minimum dimensions: minimum dimensions: nl-t; required dimensions: nl-t-yv
    copy_template_columns(df, template)
    df = df.assign(year_vtg="broadcast", year_act="broadcast", value=None)
    df.to_csv(config_dir / "var_cost_pipe_exp_edit.csv", index=False)
    log.info("Var cost pipe exp csv generated.")
    set_tech.extend(df["technology"].unique())  # set_level.extend(df["level"].unique())
    df.copy()

    # Generate export pipe technology: sheet capacity_factor_exp_pipe (no need to edit)
    template = get_template(base, "capacity_factor", tech_mother_pipe)
    if spec_tech is not None:
        df = spec_tech
    else:
        df = pd.DataFrame(
            {
                "node_loc": [node for node in node_name for _ in tech_pipe_name],
                "technology": [tech for _ in node_name for tech in tech_pipe_name],
            }
        )  # minimum dimensions: nl-t; required dimensions: nl-t-yv-ya-m-h
    copy_template_columns(df, template)
    df = df.assign(year_vtg="broadcast", year_act="broadcast", value=1)
    df.to_csv(config_dir / "capacity_factor_pipe_exp.csv", index=False)
    log.info("Capacity factor pipe exp csv generated.")
    set_tech.extend(df["technology"].unique())  # set_level.extend(df["level"].unique())
    df.copy()

    # Generate import pipe technology: name techs and levels
    tech_pipe_name = f"{tech_shorten_mother_pipe[0]}_{tech_suffix_pipe[0]}_imp"

    # Generate import pipe technology: sheet input_imp_pipe (no need to edit)
    template = get_template(base, "input", tech_mother_pipe)
    if spec_tech is not None:
        df = spec_tech
    else:
        df = pd.DataFrame(
            {
                "node_loc": [node for node in node_name for _ in tech_pipe_name],
                "technology": [tech for _ in node_name for tech in tech_pipe_name],
            }
        )
    copy_template_columns(df, template)
    df = df.assign(
        technology=tech_pipe_name,
        year_vtg="broadcast",
        year_act="broadcast",
        mode="M1",
        node_origin="R12_GLB",
        commodity=commodity_mother_pipe[0],
        level=df["node_loc"].apply(
            lambda x: f"{level_shorten_mother_pipe[0]}_{x.split('_')[1]}"
        ),
        value=1,
    )
    df.to_csv(config_dir / "input_pipe_imp.csv", index=False)
    log.info("Input pipe imp csv generated.")
    set_tech.extend(df["technology"].unique())
    set_level.extend(df["level"].unique())
    df.copy()

    # Generate import pipe technology: sheet output_imp_pipe (no need to edit)
    template = get_template(base, "output", tech_mother_pipe)
    if spec_tech is not None:
        df = spec_tech
    else:
        df = pd.DataFrame(
            {
                "node_loc": [node for node in node_name for _ in tech_pipe_name],
                "technology": [tech for _ in node_name for tech in tech_pipe_name],
            }
        )
    copy_template_columns(df, template)
    df = df.assign(
        technology=tech_pipe_name,
        year_vtg="broadcast",
        year_act="broadcast",
        mode="M1",
        node_dest=df["node_loc"],
        commodity=commodity_mother_pipe[0],
        level=level_mother_pipe[0],
        value=1,
    )
    df.to_csv(config_dir / "output_pipe_imp.csv", index=False)
    log.info("Output pipe imp csv generated.")
    set_tech.extend(df["technology"].unique())
    set_level.extend(df["level"].unique())
    df.copy()

    # Generate key relation: pipe -> pipe_group,
    # i.e, grouping exporting pipe technologies
    spec_tech_pipe_group = config.get("spec", {}).get("spec_tech_pipe_group", True)
    if spec_tech_pipe_group is True:
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
    else:
        pass  # Skip relation_tech_group processing when spec_tech_pipe_group is False

    # TODO add general function, group all pipe technologies to inter, linking inter to
    #      pipe supply techs

    # Only process relation_tech_group if it was defined
    if spec_tech_pipe_group:
        df = relation_tech_group.copy()
        set_tech.extend(df["technology"].unique())
        set_relation.extend(df["relation"].unique())

    # Generate pipe supply technology: name techs and levels
    node_name_base = base.set("node")
    node_name = {
        node
        for node in node_name_base
        if node.lower() != "world" and "glb" not in node.lower()
    }
    tech_supply_name = [
        f"{tech}_{tech_suffix_supply[0]}" for tech in tech_mother_supply
    ]

    # Generate pipe supply technology: sheet output_pipe_supply (no need to edit)
    template = get_template(base, "output", tech_mother_supply[0])
    df = pd.DataFrame(
        {
            "node_loc": [node for node in node_name for _ in tech_supply_name],
            "technology": [tech for _ in node_name for tech in tech_supply_name],
        }
    )
    copy_template_columns(df, template)
    df = df.assign(
        year_vtg="broadcast",
        year_act="broadcast",
        mode="M1",
        node_dest=df["node_loc"],
        commodity=commodity_mother_supply[0],
        level=f"{level_mother_shorten_supply[0]}_{level_suffix_supply[0]}",
        value=1,
    )
    df.to_csv(config_dir / "output_pipe_supply.csv", index=False)
    log.info("Output pipe supply csv generated.")
    set_tech.extend(df["technology"].unique())
    set_level.extend(df["level"].unique())
    df.copy()

    # Generate pipe supply technology:
    # sheet technical_lifetime_pipe_supply (no need to edit)
    df = base.par("technical_lifetime", filters={"technology": tech_mother_supply})
    df["technology"] = df["technology"].astype(str) + f"_{tech_suffix_supply[0]}"
    df.to_csv(config_dir / "technical_lifetime_pipe_supply.csv", index=False)
    log.info("Technical lifetime pipe supply csv generated.")
    set_tech.extend(df["technology"].unique())
    df.copy()

    # Generate pipe supply technology:
    # sheet inv_cost_pipe_supply (no need to edit)
    df = base.par("inv_cost", filters={"technology": tech_mother_supply})
    df["technology"] = df["technology"].astype(str) + f"_{tech_suffix_supply[0]}"
    df["value"] = df["value"] * 1  # TODO: debugging
    df.to_csv(config_dir / "inv_cost_pipe_supply.csv", index=False)
    log.info("Inv cost pipe supply csv generated.")
    set_tech.extend(df["technology"].unique())
    df.copy()

    # Generate pipe supply technology:
    # sheet fix_cost_pipe_supply (no need to edit)
    df = base.par("fix_cost", filters={"technology": tech_mother_supply})
    df["technology"] = df["technology"].astype(str) + f"_{tech_suffix_supply[0]}"
    df["value"] = df["value"] * 1  # TODO: debugging
    df.to_csv(config_dir / "fix_cost_pipe_supply.csv", index=False)
    log.info("Fix cost pipe supply csv generated.")
    set_tech.extend(df["technology"].unique())
    df.copy()

    # Generate pipe supply technology:
    # sheet var_cost_pipe_supply (no need to edit)
    df = base.par("var_cost", filters={"technology": tech_mother_supply})
    df["technology"] = df["technology"].astype(str) + f"_{tech_suffix_supply[0]}"
    df["value"] = df["value"] * 1  # TODO: debugging
    df.to_csv(config_dir / "var_cost_pipe_supply.csv", index=False)
    log.info("Var cost pipe supply csv generated.")
    set_tech.extend(df["technology"].unique())
    df.copy()

    # Generate pipe supply technology:
    # sheet capacity_factor_pipe_supply (no need to edit)
    df = base.par("capacity_factor", filters={"technology": tech_mother_supply})
    df["technology"] = df["technology"].astype(str) + f"_{tech_suffix_supply[0]}"
    df.to_csv(config_dir / "capacity_factor_pipe_supply.csv", index=False)
    log.info("Capacity factor pipe supply csv generated.")
    set_tech.extend(df["technology"].unique())
    df.copy()

    # Generate key relation: pipe_supply -> pipe,
    # i.e, pipe_supply techs contribute to pipe (group)
    spec_supply_pipe_group = config.get("spec", {}).get("spec_supply_pipe_group", True)
    if spec_supply_pipe_group is True:
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
    else:
        pass
        # TODO add general funtion, group all pipe technologies to inter, linking inter
        #      to pipe supply techs

    # Only process relation_tech_group if it was defined
    if spec_supply_pipe_group:
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
    scen: "message_ix.Scenario", config_name: Optional[str] = None
) -> "message_ix.Scenario":
    """Read the input csv files and build the pipe tech sets and parameters.

    Args:
        scen: The target scenario object to build inter_pipe on
        config_name (str, optional): Name of the config file.
            If None, uses default config from data/inter_pipe/config.yaml
    """
    # Read all CSV files that do not contain "edit" in the name
    csv_files = [
        f
        for f in Path(package_data_path("inter_pipe")).glob("*.csv")
        if "edit" not in f.name
    ]
    data_dict = {}
    for csv_file in csv_files:
        key = csv_file.stem
        data_dict[key] = pd.read_csv(csv_file)

    # Load the config
    if config_name is None:
        config_name = "config.yaml"
    full_path = package_data_path("inter_pipe", config_name)
    load_config(full_path)
    log.info(f"Loading config from: {full_path}")

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
    if info.y0 != 2030:
        raise NotImplementedError(f"inter_pipe_build() with y₀ = {info.y0} != 2030")

    # Broadcast the data
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
    for i in [k for k in data_dict.keys() if "relation_activity" in k]:
        if i != "relation_activity_ori":
            key_name = i
            df = data_dict[i][["relation", "node_rel", "year_rel", "unit"]].assign(
                value=0
            )
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
