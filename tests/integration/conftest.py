import pytest


def pytest_collection_modifyitems(items):
    """Every test in this folder needs the full docker compose stack."""
    for item in items:
        item.add_marker(pytest.mark.integration)
