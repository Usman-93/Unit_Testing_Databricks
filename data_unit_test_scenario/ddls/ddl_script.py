from data_unit_test_scenario.manage_sparkSession import spark

# COMMAND ----------
spark.sql("""
CREATE SCHEMA IF NOT EXISTS silver;
""")

# COMMAND ----------
spark.sql("""
CREATE SCHEMA IF NOT EXISTS gold;
""")

# COMMAND ----------
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.customer (
        Id INT,
        Name STRING,
        City STRING,
        CustomerCode STRING
        )
""")

# COMMAND ----------
spark.sql("""CREATE TABLE IF NOT EXISTS gold.dim_customer (
    Key BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
    Id INT,
    Name STRING,
    City STRING,
    CustomerCode STRING
    )
""")




