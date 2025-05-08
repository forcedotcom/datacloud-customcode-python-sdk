"""Version information for the Data Cloud Custom Code SDK."""

import importlib.metadata


def get_version() -> str:
    """Get the current version of the SDK.

    Returns:
        str: The version string, either from package metadata.
    """
    # First try to get version from installed package metadata
    return importlib.metadata.version("sfdc-datacloud-customcode-sdk")
