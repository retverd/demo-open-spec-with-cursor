import os

import pytest


def pytest_runtest_setup(item):
    if "integration" in item.keywords and not os.getenv("PYTEST_RUN_INTEGRATION"):
        pytest.skip("integration tests skipped (set PYTEST_RUN_INTEGRATION=1 to run)")
