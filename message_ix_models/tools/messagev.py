"""Tools for extracting data from MESSAGE V."""

import re
from functools import lru_cache

import numpy as np
import pandas as pd


class CHNFile:
    """Reader for MESSAGE V ``.chn`` files."""

    index = {}

    # FIXME reduce complexity from 15 to ≤14
    def __init__(self, path):  # noqa: C901
        """Parse .chn file."""

        def _depth(str):
            return (len(str.replace("\t", "    ")) - len(str.lstrip())) // 2

        stack = []
        self.data = {}
        for line in open(path):
            if line.startswith("#"):  # Comment
                pass
            elif len(stack) == 0:  # New level
                name, level = line.split()
                stack.append((name, level.rstrip(":")))
            elif len(stack) == 1:  # New commodity
                stack.append(tuple(line.split()))
            elif len(stack) == 2:
                if _depth(line) == 0 and line.strip() == "*":  # End of level
                    stack = []
                elif _depth(line) == 1:  # New commodity
                    stack[-1] = tuple(line.split())
                else:
                    p_c = line.strip().rstrip(":")
                    if p_c in ("Producers", "Consumers"):  # Start of P/C block
                        stack.append(p_c)
                        pc_data = []
                    elif p_c == "*":  # Consecutive '*'
                        pass
            elif len(stack) == 3:
                if _depth(line) == 2 and line.strip() == "*":  # End of block
                    # Store data
                    if len(pc_data):
                        key = tuple([stack[0][0], stack[1][0], stack[2]])
                        self.data[key] = pc_data
                    stack.pop(-1)
                else:  # Data line
                    # TODO parse: tec, level, code, commodity, {ts,c}, [data]
                    pc_data.append(line.split())
            elif line == "*\n":
                stack.pop(-1)


class DICFile:
    """Reader for MESSAGE V ``.dic`` files."""

    tec_code = {}
    code_tec = {}

    def __init__(self, path=None):
        if path is None:
            return

        for line in open(path):
            if line.startswith("#"):
                continue

            tec, code = line.split()
            self.tec_code[tec] = code
            self.code_tec[code] = tec

    def __getitem__(self, key):
        try:
            return self.code_tec[key]
        except KeyError:
            return self.tec_code[key]


class INPFile:
    """Reader for MESSAGE V ``.inp`` files."""

    index = {}
    file = None
    years_re = re.compile(r"^timesteps:(( \d*)*)", re.MULTILINE)

    def __init__(self, path):
        self.file = open(path)

        # Index the file
        section = "_info"
        pos = self.file.tell()
        while True:
            line = self.file.readline()
            if line == "":
                break
            elif line == "*\n":
                self.index[section] = (pos, self.file.tell() - pos)
                section = None
                pos = self.file.tell()
            elif section is None:
                section = line.split()[0]

    def get_section(self, name):
        start, len = self.index[name]
        self.file.seek(start)
        return self.file.read(len)

    @lru_cache(1)
    def get_years(self):
        """Return timesteps."""
        sec = self.get_section("_info")
        match = self.years_re.search(sec)
        return list(map(int, match.groups()[0].strip().split()))

    params_with_source = "con1a con1c con2a inp minp moutp"
    ts_params = "ctime fom inv plf pll vom" + params_with_source
    scalar_params = {
        "annualize": int,
        "display": str,
        "fyear": int,
        "lyear": int,
        "hisc": float,
        "minp": float,
    }

    def const_or_ts(self, line):
        param = line.pop(0)
        source = line.pop(0) if param in self.params_with_source else None

        if param == "minp":
            line = ["c" if len(line) == 1 else "ts"] + line

        kind = line.pop(0)
        if kind == "ts":
            elem = list(zip(self.get_years(), line))
        elif kind == "c":
            assert len(line) == 1

            # # This line implements a fill-forward:
            # elem = [(year, line[0]) for year in self.get_years()]

            # Single element
            elem = [(self.get_years()[0], line[0])]
        else:
            raise ValueError(param, source, kind, line)

        # 'free' is a special value for bounds/constraints
        df = (
            pd.DataFrame(elem, columns=["year", "value"])
            .replace("free", np.nan)
            .astype({"value": float})
        )

        # Add parameter name and source
        df["param"] = param
        df["source"] = source

        return df

    def parse_section(self, name):
        result = {}
        params = []

        # Parse each line
        for line in map(str.split, self.get_section(name).split("\n")):
            if line in ([], ["*"]) or line[0].startswith("#"):
                # End of section, comment, or blank line
                continue

            param = line[0]
            if param == name:
                # Start of the section
                result["extra"] = line[1:]
            elif param in "bda bdc":
                result["type"] = line.pop(1)  # 'lo' or 'hi'
                params.append(self.const_or_ts(line))
            # elif param in 'mpa mpc':
            #     # TODO implement this
            #     continue
            elif param in self.ts_params:
                params.append(self.const_or_ts(line))
            elif param in self.scalar_params:
                assert len(line) == 2, line
                result[param] = self.scalar_params[param](line[1])

        # Concatenate accumulated params to a single DataFrame
        if len(params):
            result["params"] = pd.concat(params, sort=False)

        return result
