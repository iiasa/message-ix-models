import numbers

import pandas as pd

from .get_nodes import get_nodes

# default dataframe index
df_idx = ["Model", "Scenario", "Region", "Variable", "Unit"]

# Index for iamc
iamc_idx = ["Model", "Scenario", "Region", "Variable"]

all_gases = sorted(
    ["BC", "CH4", "CO2", "N2O", "CO", "NOx", "OC", "Sulfur", "NH3", "VOC"]
)


def closest(List, K):
    """Finds the member of a list closest to a value (k)"""
    return List[min(range(len(List)), key=lambda i: abs(List[i] - K))]


def f_index(df1, df2):
    """Checks the index of two dataframes"""

    return df1.loc[df1.index.isin(df2.index)]


def f_slice(df, idx, level, locator, value):
    """Slices a MultiIndex dataframe and setting a value to a specific level

    Parameters
    ----------
    df: dataframe
    idx: list
    level: string
    locator: list,
    value: integer/string
    """
    df = df.reset_index().loc[df.reset_index()[level].isin(locator)].copy()
    df[level] = value
    return df.set_index(idx)


def idx_memb(List, x, distance):
    """Retrurns the member of the list with distance from x"""

    if List.index(x) + distance < len(List):
        return List[List.index(x) + distance]
    else:
        return False


def intpol(y1, y2, x1, x2, x, dataframe=False):
    """Interpolate between (*x1*, *y1*) and (*x2*, *y2*) at *x*.

    Parameters
    ----------
    y1, y2 : float or pd.Series
    x1, x2, x : int
    dataframe : boolean (default=True)
        Option to consider checks appropriate for dataframes/series or not.
    """
    if dataframe is False and x2 == x1 and y2 != y1:
        print(">>> Warning <<<: No difference between x1 and x2," "returned empty!!!")
        return []
    elif dataframe is False and x2 == x1 and y2 == y1:
        return y1
    else:
        if x2 == x1 and dataframe is True:
            return y1
        else:
            y = y1 + ((y2 - y1) / (x2 - x1)) * (x - x1)
            return y


def CAGR(first, last, periods):
    """Calculate Annual Growth Rate

    Parameters
    ----------
    first : number
        value of the first period
    second : number
        value of the second period
    periods : number
        period length between first and second value

    Returns
    -------
    val : number
        calculated annual growth rate
    """

    val = (last / first) ** (1 / periods)
    if not isinstance(val, numbers.Number):
        val = val.rename(last.name)
    return val


def retrieve_region_mapping(scen, mp=None, include_region_id=True):
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
        import ixmp

        mp = ixmp.Platform()
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


def rename_timeseries_regions(df, mp=None):
    """Renames the timeseries region names.

    The timeseries region names are renamed from the synonyms to the
    scenario names as mapped in the database.

    Parameters
    ----------
    df : `class`:pandas.DataFrame
        Dataframe containing timeseries data.
    mp : `class`:ixmp.Platform
        Platform from which region defintions are retrieved.
    """

    if mp is None:
        import ixmp

        mp = ixmp.Platform()
    mapping = mp.regions()
    mapping = mapping.loc[mapping.mapped_to.isin(df.region.unique())]
    mapping = mapping[["mapped_to", "region"]].set_index("mapped_to").to_dict()
    df["region"] = df["region"].map(mapping["region"])
    return df


#  def convert_timeseries_units(df, unit_in, unit_out, conversion_factor):
#
#
#
def retrieve_hierarchy(variables, splitter="|"):
    """Create a hierarchy of variables.

    For a given list of variables, derive all unique parent variables.

    variables : string or list
        Child variable(s) for which parent variables should be derived.
    splitter : string
        String by which the variable should be split
    """

    hierarchy = []

    # Ensure `variables` is a list
    if type(variables) != list:
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


def expand_gases(df, gases=None):
    """Replace all values of XXX in a dataframe with gas for all gases"""
    gases = all_gases if gases is None else gases
    dfs = [df.applymap(lambda x: x.replace("XXX", gas)) for gas in gases]
    return pd.concat(dfs, ignore_index=True, axis=0, sort=True)


def gases(var_col):
    """The gas associated with each variable"""
    gasidx = lambda x: x.split("|").index("Emissions") + 1
    return var_col.apply(lambda x: x.split("|")[gasidx(x)])


def isstr(x):
    try:
        return isinstance(x, (str, unicode))
    except NameError:
        return isinstance(x, str)


def unit_uniform(df):
    """Unifroming the "unit" in different years to prevent
    mistakes in indexing and grouping
    """

    column = [x for x in df.columns if x in ["commodity", "emission"]]
    if column:
        com_list = set(df[column[0]])
        for com in com_list:
            df.loc[df[column[0]] == com, "unit"] = df.loc[
                df[column[0]] == com, "unit"
            ].mode()[0]
    else:
        df["unit"] = df["unit"].mode()[0]
    return df


class EmissionsAggregator(object):
    def __init__(self, df, model=None, scenario=None, xlsx=None, mapping=None):
        self.multi_idx = isinstance(df.index, pd.MultiIndex)
        if self.multi_idx:
            df = df.reset_index()
        self.df = df
        self.model = model
        self.scenario = scenario
        self.xlsx = xlsx
        self.mapping = mapping

    def add_variables(self, aggregates=True):
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
            self._add_aggregates(self.mapping)
        return self

    def _add_aggregates(self, mapping):
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
            self.df = self.df.set_index(iamc_idx)
            tmp = (
                tmp.groupby(df_idx)
                .sum(numeric_only=True)
                .reset_index()
                .set_index(iamc_idx)
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
            subset = subset.groupby(df_idx).sum(numeric_only=True).reset_index()
            rows = pd.concat([rows, subset])

            self.df = self.df.set_index(iamc_idx)
            rows = rows.set_index(iamc_idx)
            self.df = rows.combine_first(self.df).reset_index()
