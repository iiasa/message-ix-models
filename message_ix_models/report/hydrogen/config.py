from dataclasses import dataclass, field
from itertools import count, product
from typing import Literal

import pandas as pd
from ixmp.report.common import RENAME_DIMS

from message_ix_models.util import package_data_path


@dataclass
class Config:
    """Configuration for reporting of a subset of hydrogen data."""

    #: Prefix or initial fragment of IAMC ‘variable’ name.
    iamc_prefix: str

    #: Units of measure for the reported data.
    unit: Literal["Mt/yr", "GWa", "Mt CH4/yr", "GW", "Mt H2/yr"]

    #: :mod:`message_ix.report` key from which to retrieve the data.
    var: Literal["out", "in", "ACT", "emi", "CAP"]

    #: Data frame with:
    #:
    #: - MultiIndex levels including 1 or more of :math:`(c, l, m, t)`.
    #: - 5 columns:
    #:   - "iamc_name": a (fragment of) an IAMC 'variable' name. This is appended to
    #:     to :attr:`iamc_prefix` to construct a complete name.
    #:   - "short_name": …
    #:   - "unit": units of measure for output.
    #:   - "original_unit": units of measure from MESSAGE-IX reporter.
    #:   - "stoichiometric_factor": optional factor to convert output commodity to
    #:     hydrogen content (applied after unit conversion, defaults to 1.0).
    #:
    #: This expresses a mapping between the index entries (=indices of reported data)
    #: and the information in the 5 columns.
    mapping: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(
            columns=[
                "iamc_name",
                "short_name",
                "unit",
                "original_unit",
                "stoichiometric_factor",
            ],
        )
    )

    @classmethod
    def from_files(cls, category: str) -> "Config":
        """Create a Config instance from 1 or 2 YAML files.

        A file like :file:`message_ix_models/data/hydrogen/reporting/{category}.yaml` is
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
        path = package_data_path("hydrogen", "reporting", f"{category}.yaml")
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
        assert self.mapping.empty or set(self.mapping.index.names) <= set("clmte")
        assert {
            "iamc_name",
            "short_name",
            "unit",
            "original_unit",
            "stoichiometric_factor",
        } == set(self.mapping.columns)
        assert not self.mapping.isna().any(axis=None)

    def use_aggregates_dict(self, data: dict) -> None:
        """Update :attr:`mapping` from `data`."""
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
                    d = dict(
                        iamc_name=k,
                        agg=v["short"],
                        short_name=v["components"],
                        original_unit=v.get("original_unit", self.unit),
                        stoichiometric_factor=v.get("stoichiometric_factor", 1.0),
                    )
                    # Convert to DataFrame with desired structure
                    dfs.append(pd.DataFrame(d))
            except KeyError:
                break  # No data for this or any subsequent levels; finish

            sn = "short_name"
            agg_mapping = (
                pd.concat(dfs)
                .merge(
                    self.mapping.reset_index().drop(
                        ["iamc_name", "original_unit", "stoichiometric_factor"], axis=1
                    ),
                    on=[sn],
                )
                .drop([sn], axis=1)
                .rename(columns={"agg": sn})
                .set_index(dims)
            )
            # Concatenate to exixsting mappings
            self.mapping = pd.concat([self.mapping, agg_mapping])

        self.check_mapping()

    def use_vars_dict(self, data: dict) -> None:
        """Update :attr:`mapping` using `data`."""

        dims: set[str] = set()
        dfs = []
        for iamc_name, values in data.items():
            filters = {
                RENAME_DIMS[k]: [v] if isinstance(v, str) else v
                for k, v in values["filter"].items()
            }
            dims |= filters.keys()

            dfs.append(
                pd.DataFrame(
                    list(product(*filters.values())), columns=list(filters.keys())
                ).assign(
                    iamc_name=iamc_name,
                    short_name=values["short"],
                    unit=values.get("unit", self.unit),
                    original_unit=values.get("original_unit", "GWa"),
                    stoichiometric_factor=values.get("stoichiometric_factor", 1.0),
                )
            )

        self.mapping = pd.concat(dfs).set_index(sorted(dims))
        self.check_mapping()
