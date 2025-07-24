import os
import sys
import pandas as pd
import logging
import yaml
import message_ix
import ixmp

mp = ixmp.Platform()
from pathlib import Path
from message_ix_models.util import package_data_path


# Get logger
def get_logger(name: str):
    # Set the logging level to INFO (will show INFO and above messages)
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)

    # Define the format of log messages:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")

    # Apply the format to the handler
    handler.setFormatter(formatter)

    # Add the handler to the logger
    log.addHandler(handler)

    return log


log = get_logger(__name__)


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


# Small util function
def broadcast_yv_ya(df: pd.DataFrame, ya_list: list[int]) -> pd.DataFrame:
    """
    Broadcast years to create vintage-activity year pairs.

    Parameters
    ----------
    df : pd.DataFrame
        Input parameter DataFrame
    ya_list : list[int]
        List of activity years to consider

    Returns
    -------
    pd.DataFrame
        DataFrame with expanded rows for each vintage-activity year pair
    """
    all_new_rows = []
    # Process each row in the original DataFrame
    for _, row in df.iterrows():
        # For each activity year
        for ya in ya_list:
            # Get all vintage years that are <= activity year
            yv_list = [yv for yv in ya_list if yv <= ya]

            # Create new rows for each vintage year
            for yv in yv_list:
                new_row = row.copy()
                new_row["year_act"] = int(ya)
                new_row["year_vtg"] = int(yv)
                all_new_rows.append(new_row)
    # Combine original DataFrame with new rows
    result_df = pd.concat([df, pd.DataFrame(all_new_rows)], ignore_index=True)
    result_df = result_df[result_df["year_vtg"] != "broadcast"]
    return result_df


# Small util function
def broadcast_yv(df: pd.DataFrame, ya_list: list[int]) -> pd.DataFrame:
    """Broadcast vintage years."""
    all_new_rows = []
    for _, row in df.iterrows():
        for yv in ya_list:
            new_row = row.copy()
            new_row["year_vtg"] = int(yv)
            all_new_rows.append(new_row)
    result_df = pd.concat([df, pd.DataFrame(all_new_rows)], ignore_index=True)
    result_df = result_df[result_df["year_vtg"] != "broadcast"]
    return result_df


# Small util function
def broadcast_yl(df: pd.DataFrame, ya_list: list[int]) -> pd.DataFrame:
    """Broadcast relation years."""
    all_new_rows = []
    for _, row in df.iterrows():
        for yv in ya_list:
            new_row = row.copy()
            new_row["year_rel"] = int(yv)
            all_new_rows.append(new_row)
    result_df = pd.concat([df, pd.DataFrame(all_new_rows)], ignore_index=True)
    result_df = result_df[result_df["year_rel"] != "broadcast"]
    return result_df


# Main function to generate bare sheets
def inter_pipe_bare(
    config_name: str = None,
    ):
    """
    Generate bare sheets to collect (minimum) parameters for pipe technologies and pipe supply technologies.

    Args:
        config_name (str, optional): Name of the config file.
            If None, uses default config from data/inter_pipe/config.yaml
    """
    # Load the config
    if config_name is None:
        config_name = "config.yaml"
    full_path = package_data_path("inter_pipe", config_name)
    config_dir = os.path.dirname(full_path)
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
    commodity_mother_pipe, commodity_suffix_pipe = (
        pipe_tech_config.get("commodity_mother_pipe", []),
        pipe_tech_config.get("commodity_suffix_pipe", []),
    )
    level_mother_pipe, level_shorten_mother_pipe, level_suffix_pipe = (
        pipe_tech_config.get("level_mother_pipe", []),
        pipe_tech_config.get("level_shorten_mother_pipe", []),
        pipe_tech_config.get("level_suffix_pipe", []),
    )
    tech_mother_supply, tech_suffix_supply, tech_number_supply = (
        pipe_supplytech_config.get("tech_mother_supply", []),
        pipe_supplytech_config.get("tech_suffix_supply", []),
        pipe_supplytech_config.get("tech_number_supply", []),
    )
    commodity_mother_supply, commodity_suffix_supply = (
        pipe_supplytech_config.get("commodity_mother_supply", []),
        pipe_supplytech_config.get("commodity_suffix_supply", []),
    )
    level_mother_supply, level_mother_shorten_supply, level_suffix_supply = (
        pipe_supplytech_config.get("level_mother_supply", []),
        pipe_supplytech_config.get("level_mother_shorten_supply", []),
        pipe_supplytech_config.get("level_suffix_supply", []),
    )

    # Load the scenario
    start_model = str(config.get("scenario", {}).get("start_model", [])[0])
    start_scen = str(config.get("scenario", {}).get("start_scen", [])[0])
    if not start_model or not start_scen:
        error_msg = (
            "Config must contain 'scenario.start_model' and 'scenario.start_scen'\n"
            f"Please check the config file at: {full_path}"
        )
        log.error(error_msg)
        raise ValueError(error_msg)
    base = message_ix.Scenario(mp, model=start_model, scenario=start_scen)
    log.info(f"Loaded scenario: {start_model}/{start_scen}")

    # Generate export pipe technology: name techs and levels
    set_tech = []
    set_level = []
    set_relation = []
    node_name_base = base.set("node")
    node_name = {
        node
        for node in node_name_base
        if node.lower() != "world" and "glb" not in node.lower()
    }
    spec_tech_pipe = str(config.get("spec", {}).get("spec_tech_pipe", [])[0])
    if spec_tech_pipe == "True":
        try:
            spec_tech = pd.read_csv(
                Path(package_data_path("inter_pipe")) / "spec_tech_pipe.csv"
            )
            tech_pipe_name = spec_tech["technology"].unique().tolist()
        except FileNotFoundError:
            spec_tech = {
                "node_loc": [],
                "technology": [],
            }
            spec_tech = pd.DataFrame(spec_tech)
            spec_tech.to_csv(
                os.path.join(config_dir, "spec_tech_pipe_edit.csv"), index=False
            )
            raise Exception(
                "The function stopped. Sheet spec_tech_pipe.csv has been generated. Fill in the specific pairs first and run again."
            )
    elif spec_tech_pipe == "False":
        tech_pipe_name = [
            f"{tech_shorten_mother_pipe[0]}_{tech_suffix_pipe[0]}_exp_{node.split('_')[1]}_{i}"
            for node in node_name
            for i in range(1, tech_number_pipe[0] + 1)
        ]
        spec_tech = None
    else:
        raise Exception("Please use True or False.")

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
    df["year_vtg"] = "broadcast"
    df["year_act"] = "broadcast"
    df["mode"] = "M1"
    df["node_origin"] = df["node_loc"]
    df["commodity"] = commodity_mother_pipe[0]
    df["level"] = f"{level_shorten_mother_pipe[0]}_{level_suffix_pipe[0]}"
    df["value"] = None
    config_dir = os.path.dirname(full_path)
    df.to_csv(os.path.join(config_dir, "input_pipe_exp_edit.csv"), index=False)
    log.info(f"Input pipe exp csv generated at: {config_dir}.")
    set_tech.extend(df["technology"].unique())
    set_level.extend(df["level"].unique())
    input_pipe_exp = df.copy()

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
    df["year_vtg"] = "broadcast"
    df["year_act"] = "broadcast"
    df["mode"] = "M1"
    df["node_dest"] = "R12_GLB"
    df["commodity"] = commodity_mother_pipe[0]
    df["level"] = df["node_loc"].apply(
        lambda x: f"{level_shorten_mother_pipe[0]}_{x.split('_')[1]}"
    )
    df["value"] = 1
    config_dir = os.path.dirname(full_path)
    df.to_csv(os.path.join(config_dir, "output_pipe_exp.csv"), index=False)
    log.info(f"Output pipe exp csv generated.")
    set_tech.extend(df["technology"].unique())
    set_level.extend(df["level"].unique())
    output_pipe_exp = df.copy()

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
    df["year_vtg"] = "broadcast"
    df["year_act"] = None
    df["value"] = None
    config_dir = os.path.dirname(full_path)
    df.to_csv(
        os.path.join(config_dir, "technical_lifetime_pipe_exp_edit.csv"), index=False
    )
    log.info(f"Technical lifetime pipe exp csv generated.")
    set_tech.extend(df["technology"].unique())  # set_level.extend(df["level"].unique())
    technical_lifetime_pipe_exp = df.copy()

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
    df["year_vtg"] = "broadcast"
    df["year_act"] = None
    df["value"] = None
    config_dir = os.path.dirname(full_path)
    df.to_csv(os.path.join(config_dir, "inv_cost_pipe_exp_edit.csv"), index=False)
    log.info(f"Inv cost pipe exp csv generated.")
    set_tech.extend(df["technology"].unique())  # set_level.extend(df["level"].unique())
    inv_cost_pipe_exp = df.copy()

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
    df["year_vtg"] = "broadcast"
    df["year_act"] = "broadcast"
    df["value"] = None
    config_dir = os.path.dirname(full_path)
    df.to_csv(os.path.join(config_dir, "fix_cost_pipe_exp_edit.csv"), index=False)
    log.info(f"Fix cost pipe exp csv generated.")
    set_tech.extend(df["technology"].unique())  # set_level.extend(df["level"].unique())
    fix_cost_pipe_exp = df.copy()

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
    df["year_vtg"] = "broadcast"
    df["year_act"] = "broadcast"
    df["value"] = None
    config_dir = os.path.dirname(full_path)
    df.to_csv(os.path.join(config_dir, "var_cost_pipe_exp_edit.csv"), index=False)
    log.info(f"Var cost pipe exp csv generated.")
    set_tech.extend(df["technology"].unique())  # set_level.extend(df["level"].unique())
    var_cost_pipe_exp = df.copy()

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
    df["year_vtg"] = "broadcast"
    df["year_act"] = "broadcast"
    df["value"] = 1
    config_dir = os.path.dirname(full_path)
    df.to_csv(os.path.join(config_dir, "capacity_factor_pipe_exp.csv"), index=False)
    log.info(f"Capacity factor pipe exp csv generated.")
    set_tech.extend(df["technology"].unique())  # set_level.extend(df["level"].unique())
    capacity_factor_pipe_exp = df.copy()

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
    df["technology"] = tech_pipe_name
    df["year_vtg"] = "broadcast"
    df["year_act"] = "broadcast"
    df["mode"] = "M1"
    df["node_origin"] = "R12_GLB"
    df["commodity"] = commodity_mother_pipe[0]
    df["level"] = df["node_loc"].apply(
        lambda x: f"{level_shorten_mother_pipe[0]}_{x.split('_')[1]}"
    )
    df["value"] = 1
    config_dir = os.path.dirname(full_path)
    df.to_csv(os.path.join(config_dir, "input_pipe_imp.csv"), index=False)
    log.info(f"Input pipe imp csv generated.")
    set_tech.extend(df["technology"].unique())
    set_level.extend(df["level"].unique())
    input_pipe_imp = df.copy()

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
    df["technology"] = tech_pipe_name
    df["year_vtg"] = "broadcast"
    df["year_act"] = "broadcast"
    df["mode"] = "M1"
    df["node_dest"] = df["node_loc"]
    df["commodity"] = commodity_mother_pipe[0]
    df["level"] = level_mother_pipe[0]
    df["value"] = 1
    config_dir = os.path.dirname(full_path)
    df.to_csv(os.path.join(config_dir, "output_pipe_imp.csv"), index=False)
    log.info(f"Output pipe imp csv generated.")
    set_tech.extend(df["technology"].unique())
    set_level.extend(df["level"].unique())
    output_pipe_imp = df.copy()

    # Generate key relation: pipe -> pipe_group, i.e, grouping exporting pipe technologies
    spec_tech_pipe_group = str(
        config.get("spec", {}).get("spec_tech_pipe_group", [])[0]
    )
    if spec_tech_pipe_group == "True":
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
                os.path.join(config_dir, "relation_activity_pipe_group_edit.csv"),
                index=False,
            )
            raise Exception(
                "The function stopped. Sheet relation_activity_pipe_group.csv has been generated. Fill in the specific pairs first and run again."
            )
    elif spec_tech_pipe_group == "False":
        pass  # TODO: adding general function, group all pipe technologies to inter, linking inter to pipe supply techs
    else:
        raise Exception("Please use True or False.")
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
    df["year_vtg"] = "broadcast"
    df["year_act"] = "broadcast"
    df["mode"] = "M1"
    df["node_dest"] = df["node_loc"]
    df["commodity"] = commodity_mother_supply[0]
    df["level"] = f"{level_mother_shorten_supply[0]}_{level_suffix_supply[0]}"
    df["value"] = 1
    config_dir = os.path.dirname(full_path)
    df.to_csv(os.path.join(config_dir, "output_pipe_supply.csv"), index=False)
    log.info(f"Output pipe supply csv generated.")
    set_tech.extend(df["technology"].unique())
    set_level.extend(df["level"].unique())
    output_pipe_supply = df.copy()

    # Generate pipe supply technology: sheet technical_lifetime_pipe_supply (no need to edit)
    df = base.par("technical_lifetime", filters={"technology": tech_mother_supply})
    df["technology"] = df["technology"].astype(str) + f"_{tech_suffix_supply[0]}"
    config_dir = os.path.dirname(full_path)
    df.to_csv(
        os.path.join(config_dir, "technical_lifetime_pipe_supply.csv"), index=False
    )
    log.info(f"Technical lifetime pipe supply csv generated.")
    set_tech.extend(df["technology"].unique())
    technical_lifetime_pipe_supply = df.copy()

    # Generate pipe supply technology: sheet inv_cost_pipe_supply (no need to edit)
    df = base.par("inv_cost", filters={"technology": tech_mother_supply})
    df["technology"] = df["technology"].astype(str) + f"_{tech_suffix_supply[0]}"
    df["value"] = df["value"] * 1  # TODO: debugging
    config_dir = os.path.dirname(full_path)
    df.to_csv(os.path.join(config_dir, "inv_cost_pipe_supply.csv"), index=False)
    log.info(f"Inv cost pipe supply csv generated.")
    set_tech.extend(df["technology"].unique())
    inv_cost_pipe_supply = df.copy()

    # Generate pipe supply technology: sheet fix_cost_pipe_supply (no need to edit)
    df = base.par("fix_cost", filters={"technology": tech_mother_supply})
    df["technology"] = df["technology"].astype(str) + f"_{tech_suffix_supply[0]}"
    df["value"] = df["value"] * 1  # TODO: debugging
    config_dir = os.path.dirname(full_path)
    df.to_csv(os.path.join(config_dir, "fix_cost_pipe_supply.csv"), index=False)
    log.info(f"Fix cost pipe supply csv generated.")
    set_tech.extend(df["technology"].unique())
    fix_cost_pipe_supply = df.copy()

    # Generate pipe supply technology: sheet var_cost_pipe_supply (no need to edit)
    df = base.par("var_cost", filters={"technology": tech_mother_supply})
    df["technology"] = df["technology"].astype(str) + f"_{tech_suffix_supply[0]}"
    df["value"] = df["value"] * 1  # TODO: debugging
    config_dir = os.path.dirname(full_path)
    df.to_csv(os.path.join(config_dir, "var_cost_pipe_supply.csv"), index=False)
    log.info(f"Var cost pipe supply csv generated.")
    set_tech.extend(df["technology"].unique())
    var_cost_pipe_supply = df.copy()

    # Generate pipe supply technology: sheet capacity_factor_pipe_supply (no need to edit)
    df = base.par("capacity_factor", filters={"technology": tech_mother_supply})
    df["technology"] = df["technology"].astype(str) + f"_{tech_suffix_supply[0]}"
    config_dir = os.path.dirname(full_path)
    df.to_csv(os.path.join(config_dir, "capacity_factor_pipe_supply.csv"), index=False)
    log.info(f"Capacity factor pipe supply csv generated.")
    set_tech.extend(df["technology"].unique())
    capacity_factor_pipe_supply = df.copy()

    # Generate key relation: pipe_supply -> pipe, i.e, pipe_supply techs contribute to pipe (group)
    spec_supply_pipe_group = str(
        config.get("spec", {}).get("spec_supply_pipe_group", [])[0]
    )
    if spec_supply_pipe_group == "True":
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
                os.path.join(config_dir, "relation_activity_supply_group_edit.csv"),
                index=False,
            )
            raise Exception(
                "The function stopped. Sheet relation_activity_supply_group.csv has been generated. Fill in the specific pairs first and run again."
            )
    elif spec_supply_pipe_group == "False":
        pass  # TODO: adding general funtion, group all pipe technologies to inter, linking inter to pipe supply techs
    else:
        raise Exception("Please use True or False.")
    df = relation_tech_group.copy()
    set_tech.extend(df["technology"].unique())
    set_relation.extend(df["relation"].unique())

    # Generate technology set sheet (no need to edit)
    technology = list(set(set_tech))
    df = pd.DataFrame({"technology": technology})
    df.to_csv(os.path.join(config_dir, "technology.csv"), index=False)
    log.info(f"Set technology csv generated.")

    # Generate commodity set sheet (no need to edit)

    # Generate level set sheet (no need to edit)
    level = list(set(set_level))
    df = pd.DataFrame({"level": level})
    df.to_csv(os.path.join(config_dir, "level.csv"), index=False)
    log.info(f"Set level csv generated.")

    # Generate relation set sheet (no need to edit)
    relation = list(set(set_relation))
    df = pd.DataFrame({"relation": relation})
    df.to_csv(os.path.join(config_dir, "relation.csv"), index=False)
    log.info(f"Set relation csv generated.")

    # # Keep track of all csv files
    # csv_files = []
    # csv_files.append(os.path.join(config_dir, "input_pipe_exp.csv")) #TODO: might be nice to have a csv list

    # return csv_files


def inter_pipe_build(config_name: str = None):
    """
    Read the input csv files and build the pipe tech sets and parameters.

    Args:
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
        config_name = "inter_pipe"
    full_path = package_data_path("inter_pipe", "config.yaml")
    config = load_config(full_path)
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

    # Broadcast the data
    ya_list = [
        2030,
        2035,
        2040,
        2045,
        2050,
        2055,
        2060,
        2070,
        2080,
        2090,
        2100,
        2110,
    ]  # TODO: get from config
    for i in data_dict.keys():
        if "year_rel" in data_dict[i].columns:
            if data_dict[i]["year_rel"].iloc[0] == "broadcast":
                data_dict[i] = broadcast_yl(data_dict[i], ya_list)
                data_dict[i]["year_act"] = data_dict[i]["year_rel"]
                log.info(f"Parameter {i} Broadcasted.")
        else:
            pass
        if "year_vtg" in data_dict[i].columns and "year_act" in data_dict[i].columns:
            if (
                data_dict[i]["year_vtg"].iloc[0] == "broadcast"
                and data_dict[i]["year_act"].iloc[0] == "broadcast"
            ):
                data_dict[i] = broadcast_yv_ya(data_dict[i], ya_list)
                log.info(f"Parameter {i} Broadcasted.")
            elif (
                data_dict[i]["year_vtg"].iloc[0] == "broadcast"
                and data_dict[i]["year_act"].iloc[0] != "broadcast"
            ):
                data_dict[i] = broadcast_yv(data_dict[i], ya_list)
                log.info(f"Parameter {i} Broadcasted.")
        else:
            pass

    # Generate relation upper and lower
    for i in [k for k in data_dict.keys() if "relation_activity" in k]:
        if i != "relation_activity_ori":
            key_name = i
            df = data_dict[i][["relation", "node_rel", "year_rel", "unit"]].assign(
                value=0
            )
            df = broadcast_yl(df, ya_list)
            key_name_upper = key_name.replace("activity", "upper")
            data_dict[key_name_upper] = df.copy()

            key_name_lower = key_name.replace("activity", "lower")
            data_dict[key_name_lower] = df.copy()
        else:
            pass

    # Load the scenario
    start_model = str(config.get("scenario", {}).get("start_model", [])[0])
    start_scen = str(config.get("scenario", {}).get("start_scen", [])[0])
    if not start_model or not start_scen:
        error_msg = (
            "Config must contain 'scenario.start_model' and 'scenario.start_scen'\n"
            f"Please check the config file at: {full_path}"
        )
        log.error(error_msg)
        raise ValueError(error_msg)
    base = message_ix.Scenario(mp, model=start_model, scenario=start_scen)
    log.info(f"Loaded scenario: {start_model}/{start_scen}")

    # Clone scenario
    target_model = str(config.get("scenario", {}).get("target_model", [])[0])
    target_scen = str(config.get("scenario", {}).get("target_scen", [])[0])
    scen = base.clone(target_model, target_scen, keep_solution=False)
    scen.set_as_default()
    log.info("Scenario cloned.")

    # Add set and parameter
    with scen.transact("Added"):
        for i in set_list:
            if i in data_dict:
                i_str = (
                    data_dict[i]
                    .apply(lambda row: row.astype(str).str.cat(sep=", "), axis=1)
                    .tolist()
                )  # str or list of str only
                scen.add_set(i, i_str)
                log.info(f"Set {i} added.")
            else:
                pass
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
            else:
                pass

    return scen
