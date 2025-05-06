
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.serverless(True).getOrCreate()

from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import random

def load_sample_customers(spark, num_rows: int = 100):
    code_options = ["Code1", "Code2", "Code3", "Code4", "Code5"]
    cities = ["Sydney", "Melbourne", "Brisbane", "Perth"]

    # Generate sample data
    data = [
        (
            i,
            f"Customer_{i}",
            random.choice(cities),
            random.choice(code_options)
        )
        for i in range(1, num_rows + 1)
    ]

    # Match the schema exactly as in your Delta table
    schema = StructType([
        StructField("Id", IntegerType(), True),
        StructField("Name", StringType(), True),
        StructField("City", StringType(), True),
        StructField("CustomerCode", StringType(), True),
    ])

    df = spark.createDataFrame(data, schema=schema)

    # Append to the Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("silver.customer")

    print(f"{num_rows} rows loaded into 'silver.customer'.")


load_sample_customers(spark)
