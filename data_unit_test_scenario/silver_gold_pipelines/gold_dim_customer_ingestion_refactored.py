from delta.tables import DeltaTable
from pyasn1_modules.rfc8398 import on_SmtpUTF8Mailbox
from pyspark.sql import functions as F

from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.serverless(True).getOrCreate()


# COMMAND ----------
def load_gold_dim_customer():
    silver_customer = customer_business_transformation()

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



def customer_business_transformation(customer=None):

    silver_customer = customer or spark.read.table("silver.customer")


    silver_customer = (
        silver_customer
        .filter(~((F.col("City") == "Sydney") & (F.col("CustomerCode") == "Code5")))


    )

    return silver_customer


load_gold_dim_customer()