from pathlib import Path


# Path for metadata
DATA_PATH = Path(__file__).parents[3] / 'data' / 'transport'


# Model & scenario names
MODEL = {
    'message-transport': dict(
        model='MESSAGEix-Transport',
        scenario='baseline',
        version=1,
        ),
    # For cloning; as suggested by OF
    # TODO find a 'clean-up' version to use
    'base': dict(
        model='CD_Links_SSP2_v2',
        scenario='baseline',
        version='latest',
        ),
}
