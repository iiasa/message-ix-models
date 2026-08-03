import pytest

from message_ix_models.report.hydrogen.config import Config
from message_ix_models.report.hydrogen.h2_reporting import (
    fetch_variables,
    load_config,
)

EFC_REPORTING_DOMAINS = (
    "hydrogen",
    "power",
    "chemicals",
    "transport",
    "industry",
)


def test_original_unit_required() -> None:
    config = Config(iamc_prefix="out|", unit="EJ/yr", var="out")
    variable = {
        "filter": {"technology": "test_technology"},
        "short": "test_variable",
    }

    with pytest.raises(
        ValueError,
        match="Reporting variable 'Test' must declare 'original_unit'",
    ):
        config.use_vars_dict({"Test": variable})


@pytest.mark.parametrize("domain", EFC_REPORTING_DOMAINS)
def test_efc_configs_declare_original_unit(domain: str) -> None:
    categories = fetch_variables(domain)
    assert categories

    for category in categories:
        config = load_config(category, domain=domain)
        assert config.mapping["original_unit"].notna().all(), f"{domain}/{category}"
