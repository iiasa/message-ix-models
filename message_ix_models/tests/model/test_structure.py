import re
from pathlib import Path

import pytest
from iam_units import registry
from sdmx.model import Annotation, Code

from message_ix_models.model.structure import (
    codelists,
    generate_set_elements,
    get_codes,
    get_region_codes,
    process_commodity_codes,
    process_units_anno,
)
from message_ix_models.util import as_codes, eval_anno


@pytest.mark.parametrize(
    "kind, exp",
    [
        ("node", ["ADVANCE", "ISR", "R11", "R12", "R14", "R32", "RCP", "ZMB"]),
        ("year", ["A", "B"]),
    ],
)
def test_codelists(kind, exp):
    """:func:`codelists` returns the expected IDs."""
    assert exp == codelists(kind)


class TestGetCodes:
    """Test :func:`get_codes` for different code lists."""

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
            "node/ZMB",
            "technology",
            "year/A",
            "year/B",
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
        assert isinstance(eval_anno(coal, "units"), registry.Unit)

        # Descriptions are parsed without new lines
        crudeoil = data[data.index("crudeoil")]
        assert "\n" not in str(crudeoil.description)

        # Processing a second time does not double-wrap the unit expressions
        process_commodity_codes(data)
        coal = data[data.index("coal")]
        assert isinstance(eval_anno(coal, "units"), registry.Unit)

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

        # get_region_codes() also works
        region_codes = get_region_codes(codelist)
        assert to_check in region_codes
        # Does not include codes that are not children of "World"
        assert member not in region_codes

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

    @pytest.mark.parametrize("codelist, length", [("A", 16), ("B", 28)])
    def test_year(self, codelist, length):
        """Year code lists can be loaded and contain the correct number of codes.

        :seealso: :meth:`.TestScenarioInfo.test_year_from_codes`.
        """
        # Year codelist can be loaded
        data = get_codes(f"year/{codelist}")

        # List contains the expected number of codes
        assert len(data) == length


def test_cli_techs(session_context, mix_models_cli):
    """Test the `techs` CLI command."""
    # Command runs without error
    result = mix_models_cli.assert_exit_0(["techs"])

    # Result test
    assert result.output.endswith("[5 rows x 8 columns]\n")

    # Path to the temporary file written by the command
    path = Path(re.match("Write to (.*.csv)", result.output)[1])

    # File was written in the local data directory
    assert Path("technology.csv") == path.relative_to(session_context.local_data)

    # File was written with the expected contents
    assert path.read_text().startswith(
        "id,name,description,type,vintaged,sector,output,input\n"
        "CF4_TCE,CF4_TCE,Tetrafluoromethane (CF4) Total Carbon Emissions,"
        "primary,False,dummy,\"['dummy', 'primary']\",\n"
    )


def test_generate_set_elements():
    data = {
        "colour": {"add": as_codes(["blue", "green", "red"])},
        "technology": {
            "add": [
                # This is configuration to generate codes based on the contents of the
                # colour set
                Code(
                    id="foo-{colour.id}",
                    annotations=[
                        Annotation(id="_generate", text=repr(dict(colour=None)))
                    ],
                ),
                Code(id="bar"),
            ]
        },
    }

    # Code runs
    generate_set_elements(data, "technology")

    # Codes are generated according to the contents of the _generate annotation
    assert {"foo-blue", "foo-green", "foo-red", "bar"} == set(
        map(str, data["technology"]["add"])
    )


def test_process_units_anno():
    # Prepare 2 codes: the parent has a units annotation, the child has none
    codes = as_codes({"foo": {"units": "kg"}, "bar": {"parent": "foo"}})

    process_units_anno("", codes[0])

    # Parents' units are propagated to the child
    assert registry.Unit("kg") == eval_anno(codes[1], "units")
