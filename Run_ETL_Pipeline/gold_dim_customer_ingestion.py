from delta.tables import DeltaTable

from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.serverless(True).getOrCreate()


# COMMAND ----------
def load_dim_customer():
    silver_customer = spark.read.table("silver.customer")

    silver_customer = (
        silver_customer
        # business_transformation
    )

    source = silver_customer

    target_table = DeltaTable.forName(spark, "gold.dim_customer")

    (
        target_table.alias("target")
        .merge(source.alias("source"),
               """
               source.Id = target.Id
               """)
        .whenMatchedUpdate(
            set = {
                "Id" : "source.Id",
                "Name" : "source.Name",
                "City" : "source.City",
                "CustomerCode" : "source.CustomerCode",
            }
        )
        .whenNotMatchedInsert(
            values={
                "Id": "source.Id",
                "Name": "source.Name",
                "City": "source.City",
                "CustomerCode": "source.CustomerCode",
            }
        )
        .execute()
    )



load_dim_customer()
