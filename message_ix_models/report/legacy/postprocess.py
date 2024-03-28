from . import pp_utils


class PostProcess(object):
    def __init__(self, ds, ix=True):
        self.ds = ds
        self.ix = ix

    # Functions which retrieve a single input parameter
    def capf(self, tec, group=["Region"]):
        """Wrapper function to retrieve capacity factor for a technology

        Parameters
        ----------
        tec : string
            technology name
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional), Vintage (optional, IX only)
        """

        tec = [tec] if type(tec) == str else tec
        df = pp_utils._retr_cpf_data(
            self.ds, self.ix, "capacity_factor", tec
        ).reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        return df.groupby(group).sum(numeric_only=True)

    def fom(self, tec, units, group=["Region", "Vintage"], formatting="standard"):
        """Wrapper function to retrieve fixed O&M cost for a technology

        Parameters
        ----------
        tec : string
            technology name
        units : string
            see unit doc
        group : list (optional, default = ['Region', 'Vintage'])
            defines for which indices the data should returned
        formatting : string (optional, default = 'standard')
            the formatting can be set to "default" in which case the
            "Vintage" will be preserved alternatively, the formatting
            can be set to "reporting" in which case values will be
            returned only for those cases where "year_act" == "year_vtg"

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional), Vintage (optional, IX only)
        """

        tec = [tec] if type(tec) == str else tec
        df = pp_utils._retr_fom_data(
            self.ds, self.ix, "fix_cost", tec, units, formatting=formatting
        ).reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        return df.groupby(group).sum(numeric_only=True)

    def inv_cost(self, tec, units, group=["Region"]):
        """Wrapper function to retrieve capacity factor for a technology

        Parameters
        ----------
        tec : string
            technology name
        units : string
            see unit doc
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional), Vintage (optional, IX only)
        """

        tec = [tec] if type(tec) == str else tec
        df = pp_utils._retr_capcost_data(self.ds, "inv_cost", tec, units).reset_index()
        return df.groupby(group).sum(numeric_only=True)

    def pll(self, tec, units, group=["Region"]):
        """Wrapper function to retrieve technical lifetime for a technology

        Parameters
        ----------
        tec : string
            technology name
        units : string
            see unit doc
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional)
        """

        tec = [tec] if type(tec) == str else tec
        df = pp_utils._retr_pll_data(
            self.ds, "technical_lifetime", tec, units
        ).reset_index()
        return df.groupby(group).sum(numeric_only=True)

    def rel(self, tec, relfilter, group=["Region"]):
        """Wrapper function to retrieve coefficient with which a
        technology writes into a given relation

        Parameters
        ----------
        tec : string
            technology name
        relfilter : dictionary
            filters specific to relation tables
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional), Mode (optional)
        """

        tec = [tec] if type(tec) == str else tec
        df = pp_utils._retr_rel_data(
            self.ds, self.ix, "relation_activity", relfilter, tec
        ).reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        return df.groupby(group).sum(numeric_only=True)

    def vom(self, tec, units, group=["Region", "Vintage"], formatting="standard"):
        """Wrapper function to retrieve variable O&M cost factor
        for a technology

        Parameters
        ----------
        tec : string
            technology name
        units : string
            see unit doc
        group : list (optional, default = ['Region', 'Vintage'])
            defines for which indices the data should returned
        formatting : string (optional, default = 'standard')
            the formatting can be set to "default" in which case the
            "Vintage" will be preserved alternatively, the formatting
            can be set to "reporting" in which case values will be
            returned only for those cases where "year_act" == "year_vtg"

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional), Vintage (optional, IX only)
        """

        tec = [tec] if type(tec) == str else tec
        df = pp_utils._retr_vom_data(
            self.ds, self.ix, "var_cost", tec, units, formatting=formatting
        ).reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        return df.groupby(group).sum(numeric_only=True)

    # Functions which retrieve mutliple parameters and perform a mathematical
    # operation with these
    def eff(
        self,
        tec,
        outfilter=None,
        inpfilter=None,
        genfilter=None,
        group=["Region"],
        formatting="standard",
    ):
        """Wrapper function to retrieve efficiency based on commodity-input
        or commodity-output coefficients for a technology

        If no input or output level/commodity is specified, then the sum over
        all inputs and outputs is calculated

        Parameters
        ----------
        tec : string
            technology name
        outfilter : dictionary
            filters specific to output tables
        inpfilter : dictionary
            filters specific to input tables
        genfilter : dictionary
            filter options that apply to input and output
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned
        formatting : string (optional, default = 'standard)
            the formatting can be set to "default" in which case the "Vintage"
            will be preserved alternatively, the formatting can be set to
            "reporting" in which case values will be returned only for those
            cases where "year_act" == "year_vtg"

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional), Mode (optional),
            Vintage (optional, IX only)
        """

        tec = [tec] if type(tec) == str else tec

        inpflt = pp_utils.combineDict(
            *[d for d in [{"technology": tec}, inpfilter, genfilter] if d is not None]
        )
        df_inp = pp_utils._retr_io_data(self.ds, self.ix, "input", inpflt, formatting)

        outflt = pp_utils.combineDict(
            *[d for d in [{"technology": tec}, outfilter, genfilter] if d is not None]
        )
        df_out = pp_utils._retr_io_data(self.ds, self.ix, "output", outflt, formatting)

        df = df_out.divide(df_inp).reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        df = df.groupby(group).sum(numeric_only=True)
        return df

    # Functions which retrieve a single ouput variable
    def act(self, tec, units=None, actfilter=None, group=["Region"]):
        """Activity for a single technology is retrieved.

        Parameters
        ----------
        tec : string or list
            technology name
        units : string (optional, default = None)
            see unit doc (if no unit is provided, no conversion will be made)
        actfilter : dictionary
            filters specific to 'reference_activity' or 'ACT' tables
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional), Mode (optional),
            Vintage (optional, IX only)
        """

        label = "ACT" if self.ix else "historical_activity"
        tec = [tec] if type(tec) == str else tec
        actflt = pp_utils.combineDict(
            *[d for d in [{"technology": tec}, actfilter] if d is not None]
        )
        df = pp_utils._retr_act_data(
            self.ds, self.ix, label, actflt, units
        ).reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        return df.groupby(group).sum(numeric_only=True)

    def cumcap(self, tec, units="GW", group=["Region"]):
        """Wrapper function to retrieve new installed capacity
        for a technology

        Parameters
        ----------
        tec : string
            technology name
        units : string (optional, default is set to GW)
            see unit doc
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional),
            Vintage (optional, IX only)
        """

        tec = [tec] if type(tec) == str else tec
        # Retrieves first year
        fyear = self.ds.set("cat_year", filters={"type_year": ["firstmodelyear"]})[
            "year"
        ].values
        # DF_hist is called with self.ix set to False so that
        # the parameter and not the variable is retrieved
        df_hist = pp_utils._retr_nic_data(
            self.ds, False, "historical_new_capacity", tec, units
        )
        # Vintage is added to DF_hist if self.ix is True because
        if self.ix:
            df_hist = df_hist.unstack(level=df_hist.index.names).reset_index()
            df_hist = df_hist.rename(columns={"level_0": "year", 0: "value"})
            df_hist["Vintage"] = df_hist["year"]
            df_hist = (
                df_hist.pivot_table(
                    values="value",
                    index=["Region", "Vintage", "Technology"],
                    columns="year",
                )
                .fillna(0)
                .reset_index()
            )
        else:
            df_hist = df_hist.reset_index()
        if "Vintage" in df_hist.columns:
            df_hist["Vintage"] = df_hist["Vintage"].astype("object")
        df_hist = df_hist.groupby(group).sum(numeric_only=True)

        label = "CAP_NEW" if self.ix else "historical_new_capacity"
        df = pp_utils._retr_nic_data(self.ds, self.ix, label, tec, units)
        df = df.reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        df = df.groupby(group).sum(numeric_only=True)
        df = df[[int(y) for y in df.columns if int(y) >= fyear]]
        df = df_hist.add(df, fill_value=0)
        # New installed capacity is reported as average yearly installed
        # capacity, so it doesnt have to be multiplied by period length.
        # If it should, use function below.
        dr_pr = self.ds.par("duration_period")
        for y in df.columns:
            if y < 2010:
                df[y] = 0
            else:
                df[y] = dr_pr[dr_pr["year"] == y]["value"].values * df[y]
        df = df.cumsum(axis=1)
        return df

    def nic(self, tec, units="GW/yr", group=["Region"]):
        """Wrapper function to retrieve new installed capacity
        for a technology

        Parameters
        ----------
        tec : string
            technology name
        units : string (optional, default is set to GW)
            see unit doc
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional), Vintage (optional, IX only)
        """

        tec = [tec] if type(tec) == str else tec
        # Retrieves first year
        fyear = self.ds.set("cat_year", filters={"type_year": ["firstmodelyear"]})[
            "year"
        ].values
        # DF_hist is called with self.ix set to False so that the parameter and
        # not the variable is retrieved
        df_hist = pp_utils._retr_nic_data(
            self.ds, False, "historical_new_capacity", tec, units
        )
        # Vintage is added to DF_hist if self.ix is True because
        if self.ix:
            df_hist = df_hist.unstack(level=df_hist.index.names).reset_index()
            df_hist = df_hist.rename(columns={"level_0": "year", 0: "value"})
            df_hist["Vintage"] = df_hist["year"]
            df_hist = (
                df_hist.pivot_table(
                    values="value",
                    index=["Region", "Vintage", "Technology"],
                    columns="year",
                )
                .fillna(0)
                .reset_index()
            )
        else:
            df_hist = df_hist.reset_index()
        if "Vintage" in df_hist.columns:
            df_hist["Vintage"] = df_hist["Vintage"].astype("object")
        df_hist = df_hist.groupby(group).sum(numeric_only=True)

        label = "CAP_NEW" if self.ix else "historical_new_capacity"
        df = pp_utils._retr_nic_data(self.ds, self.ix, label, tec, units)
        df = df.reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        df = df.groupby(group).sum(numeric_only=True)
        df = df[[int(y) for y in df.columns if int(y) >= fyear]]
        df = df_hist.add(df, fill_value=0)
        return df

    def tic(self, tec, units="GW", group=["Region"]):
        """Wrapper function to retrieve total installed capacity for a technology

        Parameters
        ----------
        tec : string
            technology name
        units : string (optional, default = 'GW')
            see unit doc
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional), Vintage (optional)
        """

        tec = [tec] if type(tec) == str else tec
        label = "CAP" if self.ix else "historical_new_capacity"
        df = pp_utils._retr_tic_data(self.ds, self.ix, label, tec, units)
        df = df.reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        df = df.groupby(group).sum(numeric_only=True)
        return df

    def extr(self, tec, units, group=["Region"]):
        """Wrapper function to retrieve extracted resource quanitites

        Parameters
        ----------
        tec : string or list
            extraction technology name
        units : string
            see unit doc
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Commodity (optional), Grade (optional)
        """

        tec = [tec] if type(tec) == str else tec
        label = "EXT" if self.ix else "historical_extraction"
        if self.ix:
            df_extr = pp_utils._retr_extr_data(self.ds, self.ix, label, tec, units)
            for yr in range(1990, 2015, 5):
                df_extr[yr] = 0.0
            df_hist = pp_utils._retr_histextr_data(
                self.ds, self.ix, "historical_extraction", tec, units
            )
            df = df_extr.add(df_hist).reset_index()
        else:
            df = pp_utils._retr_extr_data(
                self.ds, self.ix, label, tec, units
            ).reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        return df.groupby(group).sum(numeric_only=True)

    def extrc(self, tec, units, group=["Region"]):
        """Wrapper function to retrieve pre-2020 extracted resource
        quanitites (IX only)

        Parameters
        ----------
        tec : string or list
            extraction technology name
        units : string
            see unit doc
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Commodity (optional), Grade (optional)
        """

        tec = [tec] if type(tec) == str else tec
        label = "EXT" if self.ix else "historical_extraction"
        if self.ix:
            df_extr = pp_utils._retr_extr_data(self.ds, self.ix, label, tec, units)
            for yr in range(1990, 2015, 5):
                df_extr[yr] = 0.0
            df_hist = pp_utils._retr_histextr_data(
                self.ds, self.ix, "historical_extraction", tec, units
            )
            for yr in range(1990, 2005, 5):
                df_hist[yr] = 0.0
            df = df_extr.add(df_hist, fill_value=0)
        else:
            df = pp_utils._retr_extr_data(self.ds, self.ix, label, tec, units)
        df = df.reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        return df.groupby(group).sum(numeric_only=True)

    def cprc(self, units=None, group=["Region"]):
        """Wrapper function to retrieve carbon price

        Parameters
        ----------
        units : string
            see unit doc
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region
        """

        df = pp_utils._retr_crb_prc(self.ds, units).reset_index()
        return df.groupby(group).sum(numeric_only=True)

    def eneprc(self, units, enefilter=None, group=["Region"]):
        """Wrapper function to retrieve energy prices

        Parameters
        ----------
        units : string
            see unit doc
        enefilter : dictionary
            filters specific to 'PRICE_COMMODITY' tables
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional)
        """

        df = pp_utils._retr_ene_prc(self.ds, units, enefilter).reset_index()
        return df.groupby(group).sum(numeric_only=True)

    def dem(self, tec, units=None, group=["Region"], level="useful"):
        """Wrapper function to retrieve demands

        Parameters
        ----------
        tec : string
            technology name
        units : string
            see unit doc
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned
        level : string (optional, default = 'useful')
            if demands are defined on any other level than the 'useful' level,
            this can be specified using this argument

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional), Vintage (optional, IX only)
        """

        tec = [tec] if type(tec) == str else tec
        df = pp_utils._retr_demands(self.ds, self.ix, tec, level, units).reset_index()
        return df.groupby(group).sum(numeric_only=True)

    def emiss(self, emission, type_tec, group=["Region"]):
        """Wrapper function to retrieve emissions

        Parameters
        ----------
        emission : string
            emission type
        type_tec : string
            'all' or 'cumulative'
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional)
        """
        df = pp_utils._retr_emiss(self.ds, emission, type_tec).reset_index()
        return df.groupby(group).sum(numeric_only=True)

    def par_MERtoPPP(self, group=["Region"]):
        """Wrapper function to retrieve parameter conversion factor
        from GDP(MER) to GDP (PPP)

        Parameters
        ----------
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional)
        """

        df = pp_utils._retr_par_MERtoPPP(self.ds).reset_index()
        return df.groupby(group).sum(numeric_only=True)

    def var_consumption(self, group=["Region"]):
        """Wrapper function to retrieve variable "Consumption"

        Parameters
        ----------
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional)
        """

        df = pp_utils._retr_var_consumption(self.ds).reset_index()
        return df.groupby(group).sum(numeric_only=True)

    def var_gdp(self, group=["Region"]):
        """Wrapper function to retrieve variable "GDP"(MER)

        Parameters
        ----------
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional)
        """

        df = pp_utils._retr_var_gdp(self.ds, self.ix).reset_index()
        return df.groupby(group).sum(numeric_only=True)

    def var_cost(self, group=["Region"]):
        """Wrapper function to retrieve variable "COST_NODAL_NET"

        Parameters
        ----------
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional)
        """

        df = pp_utils._retr_var_cost(self.ds).reset_index()
        return df.groupby(group).sum(numeric_only=True)

    # Functions which retrieve an output VARIABLE and an input PARAMETER
    # and perform a methematical operation

    def out(self, tec, units=None, actfilter=None, outfilter=None, group=["Region"]):
        """Output for a single technology is retrieved.

        Parameters
        ----------
        tec : string or list
            technology name
        units : string (optional, default = None)
            see unit doc (if no unit is provided, no conversion will be made)
        actfilter : dictionary
            filters specific to 'reference_activity' or 'ACT' tables
        outfilter : dictionary
            filters specific to output tables
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional), Mode (optional),
            Vintage (optional, IX only)
        """

        label = "ACT" if self.ix else "historical_activity"
        tec = [tec] if type(tec) == str else tec
        actflt = pp_utils.combineDict(
            *[d for d in [{"technology": tec}, actfilter] if d is not None]
        )
        outflt = pp_utils.combineDict(
            *[d for d in [{"technology": tec}, outfilter] if d is not None]
        )
        df_act = pp_utils._retr_act_data(self.ds, self.ix, label, actflt, units)
        df_out = pp_utils._retr_io_data(self.ds, self.ix, "output", outflt)
        df = df_act.multiply(df_out).reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        return df.groupby(group).sum(numeric_only=True)

    def inp(
        self,
        tec,
        units=None,
        actfilter=None,
        inpfilter=None,
        genfilter=None,
        group=["Region"],
    ):
        """Wrapper function retrieving activity * input_efficiency
        for a given technology or list of technologies

        Parameters
        ----------
        tec : string or list
            technology name
        units : string (optional, default = None)
            see unit doc (if no unit is provided, no conversion will be made)
        actfilter : dictionary (optional, default = None)
            filters specific to 'reference_activity' or 'ACT' tables
        inpfilter : dictionary (optional, default = None)
            filters specific to input tables
        genfilter : dictionary (optional, default = None)
            filter options that apply to activity and input
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional), Mode (optional),
            Vintage (optional, IX only)
        """

        label = "ACT" if self.ix else "historical_activity"
        tec = [tec] if type(tec) == str else tec
        actflt = pp_utils.combineDict(
            *[d for d in [{"technology": tec}, actfilter, genfilter] if d is not None]
        )
        inpflt = pp_utils.combineDict(
            *[d for d in [{"technology": tec}, inpfilter, genfilter] if d is not None]
        )

        df_act = pp_utils._retr_act_data(self.ds, self.ix, label, actflt, units)
        df_inp = pp_utils._retr_io_data(self.ds, self.ix, "input", inpflt)
        df = df_act.multiply(df_inp).reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        return df.groupby(group).sum(numeric_only=True)

    def emi(
        self, tec, units, emifilter, emission_units, actfilter=None, group=["Region"]
    ):
        """Wrapper function retrieving activity * emission coefficient
        for a given technology or list of technologies

        Parameters
        ----------
        tec : string or list
            name of emission relation
        units : string
            unit into which the activity should be converted
            (note that this requires using the corresponding unit
            conversion coefficient for emissions)
        emifilter : dictionary
            filters specific to emission tables
        emission_units : string
            defines the units that the emissions should be in
        units : string
            unit into which the emissions should be converted to
        actfilter : dictionary (optional, default = None)
            filters specific to 'reference_activity' or 'ACT' tables
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional), Vintage (optional, IX only),
            Mode (optional)
        """
        label = "ACT" if self.ix else "historical_activity"
        tec = [tec] if type(tec) == str else tec
        actflt = pp_utils.combineDict(
            *[d for d in [{"technology": tec}, actfilter] if d is not None]
        )
        df_act = (
            pp_utils._retr_act_data(
                self.ds, self.ix, label, actflt, units, convert=None
            )
            .reset_index()
            .groupby(["Region", "Technology", "Mode"])
            .sum(numeric_only=True)
        )
        df_emi = pp_utils._retr_emi_data(
            self.ds, self.ix, "relation_activity", emifilter, tec, emission_units
        )
        df = df_act.multiply(df_emi, fill_value=0).reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        return df.groupby(group).sum(numeric_only=True)

    def act_rel(self, tec, relfilter, units=None, actfilter=None, group=["Region"]):
        """Wrapper function retrieving activity * relation coefficient
        for a given technology or list of technologies

        Parameters
        ----------
        tec : string or list
            name of emission relation
        relfilter : dictionary
            filters specific to relation tables
        units : string (optional)
            see unit doc
        actfilter : dictionary (optional, default = None)
            filters specific to 'reference_activity' or 'ACT' tables
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional), Mode (optional)
        """

        label = "ACT" if self.ix else "historical_activity"
        tec = [tec] if type(tec) == str else tec
        actflt = pp_utils.combineDict(
            *[d for d in [{"technology": tec}, actfilter] if d is not None]
        )
        df_act = (
            pp_utils._retr_act_data(self.ds, self.ix, label, actflt, units)
            .reset_index()
            .groupby(["Region", "Technology", "Mode"])
            .sum(numeric_only=True)
        )
        df_rel = pp_utils._retr_rel_data(
            self.ds, self.ix, "relation_activity", relfilter, tec
        )
        df = df_act.multiply(df_rel).reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        return df.groupby(group).sum(numeric_only=True)

    def act_emif(
        self,
        tec,
        emiffilter,
        units=None,
        actfilter=None,
        group=["Region"],
        emi_units=None,
    ):
        """Wrapper function retrieving activity * relation coefficient
        for a given technology or list of technologies

        Parameters
        ----------
        tec : string or list
            name of emission relation
        emiffilter : dictionary
            filters specific to emission_factor tables
        units : string
            see unit doc
        actfilter : dictionary (optional, standard = None)
            filters specific to 'reference_activity' or 'ACT' tables
        group : list (optional, standard = ['Region'])
            defines for which indices the data should returned
        emi_units : string
            see unit doc

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional), Mode (optional),
            Vintage (optional)
        """

        label = "ACT" if self.ix else "historical_activity"
        tec = [tec] if type(tec) == str else tec
        actflt = pp_utils.combineDict(
            *[d for d in [{"technology": tec}, actfilter] if d is not None]
        )
        emiflt = pp_utils.combineDict(
            *[d for d in [{"technology": tec}, emiffilter] if d is not None]
        )
        df_act = pp_utils._retr_act_data(self.ds, self.ix, label, actflt, units)
        df_emif = pp_utils._retr_emif_data(
            self.ds, self.ix, "emission_factor", emiflt, tec, emi_units
        )
        df = df_act.multiply(df_emif).reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        return df.groupby(group).sum(numeric_only=True)

    def investment(self, tec, units, group=["Region"]):
        """Wrapper function to retrieve new installed capacity for a technology

        Parameters
        ----------
        tec : string
            technology name
        units : string
            see unit doc
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional), Vintage (optional, IX only)
        """

        tec = [tec] if type(tec) == str else tec
        label = "CAP_NEW" if self.ix else "historical_new_capacity"
        df_nic = pp_utils._retr_nic_data(
            self.ds, self.ix, label, tec, "GWa"
        ).reset_index()
        if "Vintage" in df_nic.columns:
            df_nic["Vintage"] = df_nic["Vintage"].astype("object")
        df_nic = df_nic.groupby(["Region", "Technology"]).sum(numeric_only=True)

        df_capcost = pp_utils._retr_capcost_data(
            self.ds, "inv_cost", tec, units
        ).reset_index()
        if "Vintage" in df_capcost.columns:
            df_capcost["Vintage"] = df_capcost["Vintage"].astype("object")
        df_capcost = df_capcost.groupby(["Region", "Technology"]).sum(numeric_only=True)
        df = df_nic.multiply(df_capcost).reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        return df.groupby(group).sum(numeric_only=True)

    def tic_fom(self, tec, units, group=["Region"]):
        """Wrapper function to retrieve fixed O&M cost for a technology
        which is then multiplied by the total installed capacity

        Parameters
        ----------
        tec : string
            technology name
        units : string
            see unit doc
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional), Vintage (optional, IX only)
        """
        tec = [tec] if type(tec) == str else tec
        label = "CAP" if self.ix else "historical_new_capacity"
        df_tic = pp_utils._retr_tic_data(
            self.ds, self.ix, label, tec, "GW"
        ).reset_index()
        if not self.ix:
            df_tic = df_tic.groupby(["Region", "Technology"]).sum(numeric_only=True)
        else:
            df_tic = df_tic.groupby(["Region", "Technology", "Vintage"]).sum(
                numeric_only=True
            )
        df_fom = pp_utils._retr_fom_data(
            self.ds, self.ix, "fix_cost", tec, units, formatting="standard"
        )
        df = df_tic.multiply(df_fom).reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        return df.groupby(group).sum(numeric_only=True)

    def act_vom(self, tec, units, actfilter=None, group=["Region"]):
        """Wrapper function to retrieve variable O&M cost factor for
        a technology

        Parameters
        ----------
        tec : string
            technology name
        units : string
            see unit doc
        actfilter : dictionary
            filters specific to 'reference_activity' or 'ACT' tables
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional), Vintage (optional, IX only)
        """
        label = "ACT" if self.ix else "historical_activity"
        tec = [tec] if type(tec) == str else tec
        actflt = pp_utils.combineDict(
            *[d for d in [{"technology": tec}, actfilter] if d is not None]
        )
        df_act = pp_utils._retr_act_data(self.ds, self.ix, label, actflt, units="GWa")
        df_vom = pp_utils._retr_vom_data(
            self.ds, self.ix, "var_cost", tec, units, formatting="standard"
        )
        df = df_act.multiply(df_vom).reset_index()
        if "Vintage" in df.columns:
            df["Vintage"] = df["Vintage"].astype("object")
        return df.groupby(group).sum(numeric_only=True)

    # Functions that retrieve Land-use specific results
    def land_out(self, lu_out_filter, group=["Region"], units=None):
        """Wrapper function to retrieve land-use technology specific output

        Parameters
        ----------
        lu_out_filter : dictionary
            specific to 'land_output' table
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned
        units : string
            see unit doc

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional)
        """

        df_land = pp_utils._retr_land_act(self.ds, self.ix)
        df_out = pp_utils._retr_land_output(self.ds, lu_out_filter, units)
        df = df_land.multiply(df_out, fill_value=0).reset_index()
        return df.groupby(group).sum(numeric_only=True)

    def land_use(self, lu_use_filter, group=["Region"], units=None):
        """Wrapper function to retrieve land-use technology specific land use by type

        Parameters
        ----------
        lu_use_filter : dictionary
            specific to 'land_use' table
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned
        units : string
            see unit doc

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional)
        """

        df_land = pp_utils._retr_land_act(self.ds)
        df_use = pp_utils._retr_land_use(self.ds, lu_use_filter, units)
        df = df_land.multiply(df_use, fill_value=0).reset_index()
        return df.groupby(group).sum()

    def land_emi(self, tec, group=["Region"], units=None):
        """Wrapper function to retrieve land-use technology specific emissions

        Parameters
        ----------
        tec : string
            emission type
        group : list (optional, default = ['Region'])
            defines for which indices the data should returned
        units : string
            see unit doc

        Returns
        -------
        df : dataframe
            index: Region, Technology (optional)
        """

        df_land = pp_utils._retr_land_act(self.ds)
        df_emi = pp_utils._retr_land_emission(self.ds, tec, units)
        df = df_land.multiply(df_emi, fill_value=0).reset_index()
        return df.groupby(group).sum()

    def retrieve_lu_price(self, tec, scale_tec, y0=2005):
        """Wrapper function to retrieve indexed-prices.

        Indexed prices have a set of three formuals inorder to derive the sum across
        regions. Therefore this wrapper function will retrieve the regional prices
        and the quantity information, which is the processed together in a last step
        in order to derive the World value.

        Parameters
        ----------
        tec : string
            `land_output` price commodity name
        scale_tec : string
            `land_output` quantity commodity name
        y0 : int (default=2005)
            initial indexing year ie. values are 1.

        Returns
        -------
        df : dataframe
            index: Region
        """

        price = self.land_out(
            lu_out_filter={"level": ["land_use_reporting"], "commodity": [tec]}
        )
        quantity = self.land_out(
            lu_out_filter={"level": ["land_use_reporting"], "commodity": [scale_tec]}
        )
        return pp_utils.globiom_glb_priceindex(self.ds, price, quantity, scale_tec, y0)
