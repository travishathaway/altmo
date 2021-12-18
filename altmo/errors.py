"""
Contains custom errors and error messages
"""

CONFIG_ERROR_MSG = (
    "altmo-config.yml not configured correctly. Please see example for a template."
)


class AltmoConfigError(Exception):
    pass
