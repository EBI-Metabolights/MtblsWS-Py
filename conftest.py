import os
import sys

sys.path.append(os.path.abspath(__file__))
pytest_plugins = [
    "tests.fixtures",
    "tests.ws.test_data.test_data_01_fixtures"
]