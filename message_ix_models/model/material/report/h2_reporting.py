from typing import List
import pyam
from message_ix.report import Reporter

# Functions from existing reporting script
from .run_reporting import load_config, pyam_df_from_rep, format_reporting_df

def run_h2_fgt_reporting(rep: Reporter, model_name: str, scen_name: str) -> pyam.IamDataFrame:
    """Generate reporting for industry hydrogen fugitive emissions."""
    var = "h2_fgt_emi"
    config = load_config(var)
    df = pyam_df_from_rep(rep, config.var, config.mapping)
    py_df = format_reporting_df(
        df, config.iamc_prefix, model_name, scen_name, config.unit, config.mapping
    )
    return py_df

def run_lh2_fgt_reporting(rep: Reporter, model_name: str, scen_name: str) -> pyam.IamDataFrame:
    """Generate reporting for industry liquefied hydrogen fugitive emissions."""
    var = "lh2_fgt_emi"
    config = load_config(var)
    df = pyam_df_from_rep(rep, config.var, config.mapping)
    py_df = format_reporting_df(
        df, config.iamc_prefix, model_name, scen_name, config.unit, config.mapping
    )
    return py_df

def run_h2_reporting(
    rep: Reporter, model_name: str, scen_name: str
) -> List[pyam.IamDataFrame]:
    """Generate all hydrogen reporting variables for a given scenario."""
    dfs = [
        run_h2_fgt_reporting(rep, model_name, scen_name),
        run_lh2_fgt_reporting(rep, model_name, scen_name),
    ]
    return dfs
