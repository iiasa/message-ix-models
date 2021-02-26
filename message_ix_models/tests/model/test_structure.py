import pytest
from iam_units import registry

from message_ix_models.model.structure import get_codes


class TestGetCodes:
    """Test :func:`get_codes() for different code lists."""

    @pytest.mark.parametrize(
        "name",
        (
            "cd_links/unit",
            "commodity",
            "level",
            "node/ISR",
            "node/R11",
            "node/R14",
            "node/R32",
            "node/RCP",
            "technology",
        ),
    )
    def test_get_codes(self, name):
        """The included code lists can be loaded."""
        get_codes(name)

    def test_hierarchy(self):
        """get_codes() returns objects with the expected hierarchical relationship."""
        codes = get_codes("node/R11")

        AUT = codes[codes.index("AUT")]
        R11_WEU = codes[codes.index("R11_WEU")]
        World = codes[codes.index("World")]

        assert R11_WEU is AUT.parent
        assert AUT in R11_WEU.child

        assert World is R11_WEU.parent
        assert R11_WEU in World.child

    def test_commodities(self):
        data = get_codes("commodity")

        # Some expected commodities are present
        for check in "coal", "electr":
            assert check in data

        # Units for one commodity can be retrieved and parsed
        coal = data[data.index("coal")]
        registry(str(coal.get_annotation(id="unit").text))

        # Descriptions are parsed without new lines
        crudeoil = data[data.index("crudeoil")]
        assert "\n" not in str(crudeoil.description)

    def test_levels(self):
        data = get_codes("level")

        # Some expected commodities are present
        for check in "primary", "useful":
            assert check in data

        # Descriptions are parsed without new lines
        assert "\n" not in str(data[data.index("primary")].description)

    @pytest.mark.parametrize(
        "codelist, to_check, length, member",
        [
            ("RCP", "R5_MAF", 69, "CIV"),
            ("R11", "R11_AFR", 50, "CIV"),
            ("R14", "R14_AFR", 51, "CIV"),
            ("R32", "R32SSA-L", 41, "CIV"),
        ],
    )
    def test_nodes(self, codelist, to_check, length, member):
        """Tests of node codelists."""
        # Node codelist can be loaded
        data = get_codes(f"node/{codelist}")

        # List contains a particular region
        assert to_check in data

        # Region contains the correct number of countries
        code = data[data.index(to_check)]
        assert len(code.child) == length

        # A specific country is present in the region
        assert member in code.child

    def test_node_historic_country(self):
        """get_codes() handles ISO 3166 alpha-3 codes for historic countries."""
        assert "SCG" in get_codes("node/R11")

    def test_technologies(self):
        # Retrieve the tech info without calling technologies.cli
        data = get_codes("technology")

        # Check the length of the returned dataframe
        assert len(data) == 377

        # Get info on a certain technology
        h2_fc_trp = data[data.index("h2_fc_trp")]
        assert ["transport", "useful"] == eval(
            str(h2_fc_trp.get_annotation(id="output").text)
        )

        # Check that the default value for 'vintaged' is False when omitted from the
        # YAML file
        elec_exp = data[data.index("elec_exp")]
        assert False is eval(str(elec_exp.get_annotation(id="vintaged").text))
