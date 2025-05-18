from data_unit_test_scenario.manage_sparkSession import spark

spark.sql(
    """
    DROP TABLE IF EXISTS silver.customer
    """
)

spark.sql(
    """
    DROP TABLE IF EXISTS gold.dim_customer
    """
)