import pytest

from databricks.connect import DatabricksSession

@pytest.fixture(scope="session")
def serverless():
    return DatabricksSession.builder.serverless(True).getOrCreate()