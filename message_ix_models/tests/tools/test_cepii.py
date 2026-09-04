import re
from typing import TYPE_CHECKING

import pytest
from genno import Computer

from message_ix_models.tools.cepii import (
    ARCHIVE,
    BACI,
    COUNTRY_CODES,
    _code_map,
    load_data,
)
from message_ix_models.util.pooch import SOURCE, fetch

if TYPE_CHECKING:
    from message_ix_models import Context


class TestBACI:
    class TestOptions:
        def test_post_init(self) -> None:
            with pytest.raises(
                ValueError,
                match=re.escape("non-existent dimension(s): ['x', 'y', 'z']"),
            ):
                BACI.Options(filter_pattern={"k": "", "x": "", "y": "", "z": ""})

        def test_release_default(self) -> None:
            """The default is the earliest release, so adding one changes nothing."""
            assert min(ARCHIVE) == BACI.Options().release

        def test_release_invalid(self) -> None:
            with pytest.raises(ValueError, match="release='202401'; expected one of"):
                BACI.Options(release="202401")

    @pytest.mark.parametrize(
        "measure",
        [
            "quantity",
            "value",
            pytest.param("foo", marks=pytest.mark.xfail(raises=ValueError)),
        ],
    )
    @pytest.mark.parametrize(
        "test_data, filter_pattern, size",
        # Subset of the product codes for MESSAGE commodity="coal"
        [
            pytest.param(
                False,
                dict(k="270(11[129]|[246]..)"),
                112319,
                marks=pytest.mark.skip(reason="High resource usage"),
            ),
            (True, dict(k="270(11[129]|[246]..)"), 110),
        ],
    )
    def test_add_tasks(
        self,
        test_context: "Context",
        measure: str,
        filter_pattern: dict,
        test_data: bool,
        size: int,
    ) -> None:
        test_context.model.regions = "R12"

        c = Computer()

        keys = BACI.add_tasks(
            c,
            context=test_context,
            measure=measure,
            filter_pattern=filter_pattern,
            test=test_data,
        )

        # Preparation of data runs successfully
        result = c.get(keys[0])

        # Data have the expected dimensions and size
        assert {"t", "i", "j", "k"} == set(result.dims)
        assert size == result.size


class TestArchive:
    def test_keys_match_registry(self) -> None:
        """Every release names a file the registry can fetch, and vice versa.

        Guards a release added to one and not the other, which would otherwise
        surface as a KeyError or a silent fallback at fetch time.
        """
        registry = set(SOURCE["CEPII_BACI"]["pooch_args"]["registry"])
        assert registry == set(ARCHIVE.values())

    def test_release_appears_in_filename(self) -> None:
        """`get()` selects data files by release substring, so this must hold."""
        for release, filename in ARCHIVE.items():
            assert f"V{release}" in filename


class TestFetch:
    """The registry holds more than one file, so `filename` is not optional."""

    def test_filename_required(self) -> None:
        with pytest.raises(ValueError, match="filename= must name one of"):
            fetch(**SOURCE["CEPII_BACI"])

    def test_filename_unknown(self) -> None:
        with pytest.raises(ValueError, match="is not in the registry"):
            fetch(**SOURCE["CEPII_BACI"], filename="BACI_HS92_V1999.zip")


class TestCodeMap:
    def test_idiosyncratic_codes_take_precedence(self) -> None:
        """The hand-listed codes must not be overwritten by the ISO 3166-1 values.

        Several deliberately differ from ISO for the same country, so the merge order
        is load-bearing: reverse it and e.g. 891 resolves to the ISO country rather
        than the historical entity that carries pre-2006 trade.
        """
        mapping = _code_map()

        for code, alpha_3 in COUNTRY_CODES:
            assert alpha_3 == mapping[code], f"{code} overwritten"

    def test_serbia_and_montenegro(self) -> None:
        assert "SCG" == _code_map()[891]


class TestLoadData:
    def test_unknown_reporter_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An unmapped reporter fails loudly rather than vanishing from the result."""
        import pandas as pd

        from message_ix_models.tools import cepii

        frame = pd.DataFrame(
            {"t": [2022, 2022], "i": [4, 8], "j": [8, 99999], "k": [270112, 270112]}
        )
        monkeypatch.setattr(cepii, "fetch", lambda **kw: ())
        monkeypatch.setattr(cepii, "baci_data_from_files", lambda *a: frame)

        with pytest.raises(ValueError, match=r"reporter\(s\) \[99999\]$"):
            load_data(release="202601")
