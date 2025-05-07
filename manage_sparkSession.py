from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.serverless(True).getOrCreate()