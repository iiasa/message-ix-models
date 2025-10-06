from dataclasses import dataclass, field
from itertools import count, product
from typing import Literal

import pandas as pd
from ixmp.report.common import RENAME_DIMS

from message_ix_models.util import package_data_path


@dataclass
class Config:
    """Configuration for reporting of a subset of material data."""

    #: Prefix or initial fragment of IAMC ‘variable’ name.
    iamc_prefix: str

    #: Units of measure for the reported data.
    unit: Literal["Mt/yr", "GWa", "Mt CH4/yr", "GW"]

    #: :mod:`message_ix.report` key from which to retrieve the data.
    var: Literal["out", "in", "ACT", "emi", "CAP"]

    #: Data frame with:
    #:
    #: - MultiIndex levels including 1 or more of :math:`(c, l, m, t)`.
    #: - 3 columns:
    #:   - "iamc_name": a (fragment of) an IAMC ‘variable’ name. This is appended to
    #:     to :attr:`iamc_prefix` to construct a complete name.
    #:   - "short_name": …
    #:   - "unit": units of measure.
    #:
    #: This expresses a mapping between the index entries (=indices of reported data)
    #: and the information in the 3 columns.
    mapping: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(
            columns=["iamc_name", "short_name", "unit"],
        )
    )

    @classmethod
    def from_files(cls, category: str) -> "Config":
        """Create a Config instance from 1 or 2 YAML files.

        A file like :file:`message_ix_models/data/material/reporting/{category}.yaml` is
        read and used to populate a new instance. The file must have:

        - Top-level keys corresponding to :attr:`iamc_prefix`, :attr:`unit`, and
          :attr:`var`.
        - A top-level key ``vars:`` containing a mapping compatible with
          :meth:`use_vars_dict`.

        If a file exists in the same directory named like
        :file:`{category}_aggregates.yaml`, it is also read, and its contents passed to
        :meth:`use_aggregates_dict`.
        """
        import yaml

        # Handle basic configuration file
        path = package_data_path("material", "reporting", f"{category}.yaml")
        with open(path) as f:  # Raises FileNotFoundError on missing file
            kw = yaml.safe_load(f)  # Raises on invalid YAML

        # Remove the "vars" top-level key from the file
        vars = kw.pop("vars")

        # Create a ReporterConfig instance
        result = cls(**kw)

        # Update mapping data frame using `vars`
        result.use_vars_dict(vars)

        # Handle aggregates configuration file
        path_agg = path.with_name(f"{category}_aggregates.yaml")
        try:
            with open(path_agg) as f:
                data_agg = yaml.safe_load(f)
        except FileNotFoundError:
            data_agg = dict()  # No aggregates file

        result.use_aggregates_dict(data_agg)

        return result

    def check_mapping(self) -> None:
        """Assert that :attr:`mapping` has the correct structure and is complete."""
        assert self.mapping.empty or set(self.mapping.index.names) <= set("clmt")
        assert {"iamc_name", "short_name", "unit"} == set(self.mapping.columns)
        assert not self.mapping.isna().any(axis=None)

    def use_aggregates_dict(self, data: dict) -> None:
        """Update :attr:`mapping` from `data`.

        This method handles `data` with structure equivalent to the following YAML
        content:

        .. code-block:: yaml

           level_1:
             Chemicals|Liquids|Other:
               short: fe_pe_chem_oth
               components: [ fe_pe_hvc_oth ]
             Chemicals|Liquids|Biomass:
               short: fe_pe_chem_bio
               components: [ fe_pe_hvc_bio_eth ]
             # Any number of similar entries
           level_2:
             Heat:
               short: fe_pe_heat
               components:
               - fe_pe_cement_heat
               - fe_pe_aluminum_heat
               - fe_pe_steel_heat
               - fe_pe_other_heat
             # Any number of similar entries

        In general:

        - Top-level keys may be "level_1", "level_2", etc. Additional top-level keys
          like "iamc_prefix", "unit", and "var" are checked against the corresponding
          attributes.
        - Second-level keys are fragments of IAMC ‘variable’ names
        - Third level keys must be:

          - "short": A single string. See the description of the "short_name" column in
            :attr:`mapping`. This is the aggregate to be produced.
          - "components": A list of strings. These are the components of the
            aggregation. Components referenced under "level_1" must already be present
            in :attr:`mapping`. Components referenced under "level_2" may include the
            aggregates described by "level_1", etc.
        """
        # Check that other entries in `data` (e.g. loaded from YAML) match
        for k in ("iamc_prefix", "unit", "var"):
            assert data.pop(k, getattr(self, k)) == getattr(self, k)

        dims = self.mapping.index.names

        # Iterate over top-level keys: "level_1", "level_2", etc.
        for k_level in map("level_{}".format, count(start=1)):
            try:
                # Iterate over aggregates defined in this "level"
                dfs = []
                for k, v in data.pop(k_level).items():
                    # Extract aggregate name and components
                    d = dict(iamc_name=k, agg=v["short"], short_name=v["components"])
                    # Convert to DataFrame with desired structure
                    dfs.append(pd.DataFrame(d))
            except KeyError:
                break  # No data for this or any subsequent levels; finish

            # The merge and concat steps must be repeated on every iteration so that
            # aggregates defined under "level_2" may refer to aggregates defined under
            # "level_1" etc.

            # - Concatenate together all `dfs`.
            # - Merge with (c, l, m, t, short_name, unit) from self.mapping (omit
            #   existing iamc_name), on the short_name values.
            # - Replace the existing short_name with aggregate short_name.
            # - Restore multiindex.
            sn = "short_name"
            agg_mapping = (
                pd.concat(dfs)
                .merge(self.mapping.reset_index().drop(["iamc_name"], axis=1), on=[sn])
                .drop([sn], axis=1)
                .rename(columns={"agg": sn})
                .set_index(dims)
            )
            # Concatenate to exixsting mappings
            self.mapping = pd.concat([self.mapping, agg_mapping])

        self.check_mapping()

    def use_vars_dict(self, data: dict) -> None:
        """Update :attr:`mapping` using `data`.

        This handles `data` with structure equivalent to the following YAML content:

        .. code-block:: yaml

           Chemicals|High-Value Chemicals|Electricity|Steam Cracking:
             filter:
               commodity: electr
               level: final
               mode: [vacuum_gasoil, atm_gasoil, naphtha, ethane, propane]
               technology: steam_cracker_petro,
             short: fe_pe_hvc_el_sc
             unit: kg  # Optional

           # Any number of similar entries

        Within this:

        - ``Chemicals|High-Value Chemicals|Electricity|Steam Cracking`` is a (fragment
          of) an IAMC ‘variable’ name.
        - ``filter`` entries may have values that are strings or lists of strings.
          The subkeys may include the MESSAGEix sets [technology, mode, commodity,
          level].
        """

        dims: set[str] = set()
        dfs = []
        for iamc_name, values in data.items():
            # Convert:
            # - scalar/single str entries to length-1 list of str
            # - long/full message_ix set names ("technology") to short dim IDs ("t")
            filters = {
                RENAME_DIMS[k]: [v] if isinstance(v, str) else v
                for k, v in values["filter"].items()
            }
            dims |= filters.keys()

            # - Create data frame: all valid combinations of indices
            # - Set other columns
            dfs.append(
                pd.DataFrame(
                    list(product(*filters.values())), columns=list(filters.keys())
                ).assign(
                    iamc_name=iamc_name,
                    short_name=values["short"],
                    unit=values.get("unit", self.unit),
                )
            )

        # Concatenate all mappings; set multi-index based on `dims`
        self.mapping = pd.concat(dfs).set_index(sorted(dims))
        self.check_mapping()
