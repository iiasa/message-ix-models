from typing import Optional, Union

import pandas as pd
from ixmp import Platform
from message_ix import Scenario

from . import ALL_GASES, DF_IDX, IAMC_IDX


def expand_gases(df: pd.DataFrame, gases=None) -> pd.DataFrame:
    """Replace all values of XXX in a dataframe with gas for all gases"""
    gases = ALL_GASES if gases is None else gases
    dfs = [df.applymap(lambda x: x.replace("XXX", gas)) for gas in gases]
    return pd.concat(dfs, ignore_index=True, axis=0, sort=True)


def gas_idx(gas_name: str) -> int:
    return gas_name.split("|").index("Emissions") + 1


def gases(var_col: pd.Series):
    """The gas associated with each variable"""
    return var_col.apply(lambda x: x.split("|")[gas_idx(x)])


def get_historical_years(scen: Scenario, year_min: int = 1990) -> list[int]:
    """Retrieves historical time periods for a given scenario.

    Parameters
    ----------
    scenario : :class:`message_ix.Scenario`
        scenario for which the historical time period should be retrieved
    year_min : int
        starting year of historical time period.

    Returns
    -------
    years : list
        all historical time periods
    """

    firstmodelyear = int(
        scen.set("cat_year", {"type_year": ["firstmodelyear"]})["year"]
    )
    model_years = [int(x) for x in scen.set("year")]
    years = [y for y in model_years if y < firstmodelyear and y >= year_min]
    return years


def get_nodes(scen: Scenario) -> list[str]:
    """Retrieve all the nodes defined in a scenario, excluding 'WORLD'.

    Parameters
    ----------
    scen : :class:`message_ix.Scenario`
        Scenario from which nodes should be retrieved.

    Returns
    -------
    list of str
        Regions in the scenario, excluding 'WORLD'.
    """
    nodes: pd.Series[str] = scen.set("node")
    return [r for r in nodes.tolist() if r not in ["World"]]


def get_optimization_years(scen: Scenario) -> list[int]:
    """Retrieves optimization horizon for a given scenario.

    Parameters
    ----------
    scen : :class:`message_ix.Scenario`
        Scenario for which the optimization period should be determined

    Returns
    -------
    years : list
        all model years for which the model will carry out the optimization
    """

    firstmodelyear = int(
        scen.set("cat_year", {"type_year": ["firstmodelyear"]})["year"]
    )
    model_years: list[int] = scen.set("cat_year").year.unique().tolist()
    years = [y for y in model_years if y >= firstmodelyear]
    return years


def retrieve_hierarchy(variables: Union[str | list], splitter: str = "|") -> list:
    """Create a hierarchy of variables.

    For a given list of variables, derive all unique parent variables.

    variables : string or list
        Child variable(s) for which parent variables should be derived.
    splitter : string
        String by which the variable should be split
    """

    hierarchy = []

    # Ensure `variables` is a list
    if isinstance(variables, list):
        variables = [variables]

    # Iterate over list
    for h in variables:
        # Split variable
        parts = str.split(h, splitter)
        # Create all possible parent variables by rejoining individual parts
        for i in range(len(parts))[1:]:
            joined = splitter.join(parts[:i])
            hierarchy.append(joined)
    return sorted(list(set(hierarchy) | set(variables)))


def retrieve_region_mapping(
    scen: Scenario, mp: Optional[Platform] = None, include_region_id: bool = True
):
    """Retrieve scenario-specific region mapping from the platform.

    Parameters
    ----------
    scen : `class`:message_ix.Scenario
        Dataframe containing timeseries data.
    mp : `class`:ixmp.Platform
        Platform from which region defintions are retrieved.
    include_region_id : boolean (default=True)
        Option whether to include region_id in the mapping or not i.e.
        whether to map to e.g. `R11_AFR` (TRUE) or `AFR` (FALSE)
    """

    if mp is None:
        mp = Platform()
    region_id = list(set([x.split("_")[0] for x in get_nodes(scen)]))[0]
    df = mp.regions()
    df = df[df.region.isin([r for r in scen.set("node") if r != "World"])]
    if include_region_id is True:
        df["region"] = df["region"].str.replace(f"{region_id}_GLB", "World")
    else:
        df["region"] = df["region"].str.replace(f"{region_id}_", "")
        df["region"] = df["region"].str.replace("GLB", "World")
    df = df[["region", "mapped_to"]].set_index("mapped_to").to_dict()["region"]
    return (region_id, df)


class EmissionsAggregator(object):
    def __init__(
        self,
        df: pd.DataFrame,
        mapping: dict,
        model: Optional[str] = None,
        scenario: Optional[str] = None,
    ):
        self.multi_idx = isinstance(df.index, pd.MultiIndex)
        if self.multi_idx:
            df = df.reset_index()
        self.df = df
        self.model = model
        self.scenario = scenario
        self.mapping = mapping

    def add_variables(self, aggregates: bool = True):
        """
        Add aggregates and variables with direct mappings.

        Parameters
        ----------
        totals: whether to add totals
        add_aggregates: optional, whether to add aggregate variables
        ceds_types: optional, string or list, whether to add CEDS variables
                    type can take on any value, but usually is Historical or
                    Unharmonized
        """
        if aggregates:
            self._add_aggregates(mapping=self.mapping)
        return self

    def _add_aggregates(self, mapping: dict):
        # Retrieves list of gases in the dataframe
        try:
            gas_list = gases(self.df.Variable).unique().tolist()
            mapping = expand_gases(mapping, gas_list)
        except Exception:
            print("no emissions included")

        # Add totals
        variables = self.df.Variable.unique().tolist()
        variable_hierarchy = retrieve_hierarchy(variables)
        variable_missing = [
            v
            for v in variable_hierarchy
            if v not in variables and v not in mapping["IAMC Parent"].unique().tolist()
        ]

        # Derive child variables for missing variables and then perform aggregation
        for var in variable_missing:
            variable_child = [
                v
                for v in variable_hierarchy
                if (len(v.split("|")) == len(var.split("|")) + 1) and var in v
            ]
            if not variable_child:
                continue
            tmp = self.df.loc[self.df.Variable.isin(variable_child)]
            tmp = tmp.assign(Variable=var)
            self.df = self.df.set_index(IAMC_IDX)
            tmp = (
                tmp.groupby(DF_IDX)
                .sum(numeric_only=True)
                .reset_index()
                .set_index(IAMC_IDX)
            )
            self.df = tmp.combine_first(self.df).reset_index()

        # rows = pd.DataFrame(columns=self.df.columns)
        # COMMENT OFR 05.05.2017: Rather than having the new dataframe (rows)
        # added after all the new variables have been compiled, the variables
        # are now added idnividually to the overall dataframe to allow multiple
        # interdependency between aggregates. i.e. a=b+c; d=a+e
        for sector in mapping["IAMC Parent"].unique():
            rows = self.df.iloc[:0, :].copy()
            # mapping for aggregate sector for all gases
            _map = mapping[mapping["IAMC Parent"] == sector]
            _map = _map.set_index("IAMC Child")["IAMC Parent"]

            # rename variable column for subset of rows
            subset = self.df[self.df.Variable.isin(_map.index)].copy()
            subset.Variable = subset.Variable.apply(lambda x: _map.loc[x])

            # add aggregate to rows
            subset = subset.groupby(DF_IDX).sum(numeric_only=True).reset_index()
            rows = pd.concat([rows, subset])

            self.df = self.df.set_index(IAMC_IDX)
            rows = rows.set_index(IAMC_IDX)
            self.df = rows.combine_first(self.df).reset_index()
