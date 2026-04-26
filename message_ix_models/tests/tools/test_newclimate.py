import pytest

from message_ix_models.testing import KEY as STASH_KEY
from message_ix_models.tools.newclimate import SECTOR, fetch, get, read
from message_ix_models.tools.newclimate.structure import STRINGENCY


class TestSTRINGENCY:
    def test_int(self) -> None:
        """Lookup of str containing only digits gives an enumeration member."""
        assert STRINGENCY["1"] == STRINGENCY._1


@pytest.mark.parametrize(
    "version",
    ("2024", "2023", "2022", "2021", "2020", "2019"),
)
def test_fetch(version: str) -> None:
    # File can be fetched
    p = fetch(version)

    assert p.exists()


@pytest.mark.parametrize(
    "version, N_total, N_transport",
    (
        ("2024", 6507, 1298),
        ("2023", 6273, 1246),
        ("2022", 5883, 1203),
        pytest.param("2021", 1, 1, marks=pytest.mark.xfail(raises=NotImplementedError)),
        pytest.param("2020", 1, 1, marks=pytest.mark.xfail(raises=NotImplementedError)),
        pytest.param("2019", 1, 1, marks=pytest.mark.xfail(raises=NotImplementedError)),
    ),
)
def test_get(version: str, N_total: int, N_transport: int) -> None:
    # Data can be fetched and read
    result = get(version)

    # Expected number of records
    N = len(result)
    assert N_total == N

    # Objects can be filtered using enumerations
    subset = {k: p for k, p in result.items() if SECTOR.Transport in p.sector}

    N = len(subset)
    assert N_transport == N


def test_read0() -> None:
    result = get("2024")

    # Retrieve one entry
    p = result["211000001"]

    # Enumerated field is parsed to a list of enum items
    assert [SECTOR.Electricity_and_heat, SECTOR.Renewables] == p.sector

    # Geo field contains a pycountry object
    assert 1 == len(p.geo)
    # …that can be used to access various fields, as needed
    assert "ITA" == p.country.alpha_3
    assert "Italy" == p.country.name


@pytest.mark.parametrize(
    "filename, N_total",
    (
        ("Canada_edits_additions0.csv", 18),
        ("Canada_edits_additions1.csv", 1),
        ("climate_policy_database_policies_2025.csv", 6507),
    ),
)
def test_read_local_data(
    pytestconfig: pytest.Config, filename: str, N_total: int
) -> None:
    """Test files in user's local data path."""
    path = pytestconfig.stash[STASH_KEY["user-local-data"]].joinpath(
        "newclimate", filename
    )

    if path.exists():
        # Function runs
        result = read(path)

        # Expected number of records
        N = len(result)
        assert N_total == N
