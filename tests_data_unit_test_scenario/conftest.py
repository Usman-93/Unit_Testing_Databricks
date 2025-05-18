import pytest
import builtins

from databricks.connect import DatabricksSession


@pytest.fixture(scope="session", autouse=True)
def global_spark_session():
    spark = DatabricksSession.builder.serverless(True).getOrCreate()
    builtins.spark = spark

    yield spark

    # Clean up
    spark.stop()
    if hasattr(builtins, 'spark'):
        delattr(builtins, 'spark')

# @pytest.fixture(scope="session")
# def serverless():
#     return DatabricksSession.builder.serverless(True).getOrCreate()