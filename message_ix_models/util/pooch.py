import click
import pooch

POOCH = pooch.create(
    path=pooch.os_cache("message-ix-models"),
    base_url="doi:10.5281/zenodo.5793870",
    registry={
        "MESSAGEix-GLOBIOM_1.1_R11_no-policy_baseline.xlsx": (
            "md5:222193405c25c3c29cc21cbae5e035f4"
        ),
    },
)


@click.group("snapshot")
def cli():
    pass


@cli.command()
def fetch():
    POOCH.fetch("MESSAGEix-GLOBIOM_1.1_R11_no-policy_baseline.xlsx")
