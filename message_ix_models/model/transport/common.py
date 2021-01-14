from message_data.model import bare

__all__ = ["SETTINGS"]

# Same settings as model.bare, except…
SETTINGS = bare.SETTINGS.copy()

# Default R11 nodes; RCP not supported
SETTINGS["region"] = ["R11", "R14", "ISR"]


# Configuration files
METADATA = [
    # Information about MESSAGE-Transport
    ("transport", "config"),
    ("transport", "set"),
    ("transport", "technology"),
    # Information about the MESSAGE V model
    ("transport", "migrate", "set"),
]
