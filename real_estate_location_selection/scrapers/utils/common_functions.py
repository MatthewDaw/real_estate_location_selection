from camoufox.sync_api import Camoufox

def safe_get(data, *keys, default=None):
    """
    Safely traverse nested dictionaries without getting KeyError or TypeError.

    Args:
        data: The data structure to traverse
        *keys: Keys to traverse in order
        default: Value to return if any key is missing or data is None

    Returns:
        The value at the nested key path, or default if not found/accessible
    """
    try:
        for key in keys:
            if data is None:
                return default
            data = data[key]
        return data
    except (KeyError, TypeError, AttributeError):
        return default


def safe_lower(value):
    """Safely convert to lowercase, handling None values."""
    return value.lower() if value is not None else None


def safe_divide(numerator, denominator):
    """Safely divide two numbers, handling None and zero cases."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    try:
        return numerator / denominator
    except (TypeError, ZeroDivisionError):
        return None

def get_browser():
    browser = Camoufox(
        humanize=True,
        firefox_user_prefs={
            "javascript.enabled": False,
            "permissions.default.image": 2,  # Block images
            "permissions.default.stylesheet": 2,  # Block CSS
            "permissions.default.font": 2,  # Block fonts
            "permissions.default.script": 2,  # Block JavaScript
            "permissions.default.plugin": 2,  # Block plugins
            "permissions.default.autoplay": 2,  # Block autoplay media
            "permissions.default.geo": 2,  # Block geolocation
        },
    ).start()
    return browser
