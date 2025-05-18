
from silver_gold_pipelines.gold_dim_customer_ingestion_refactored import customer_business_transformation

def test_gold_dim_customer_sydney_code5_no_rows():
    # STEP 1: Arrange
    # limit(0): As I want to use my own test data to this object
    silver_customer = spark.read.table("silver.customer").limit(0)

    test_data = [

        (1, "Customer_1", "Sydney", "Code5"),
        (2, "Customer_2", "Sydney", "Code5"),

        (3, "Customer_3", "Adelaide", "Code5"),
        (4, "Customer_4", "Melbourne", "Code1"),
        (5, "Customer_5", "Brisbane", "Code2"),
        (6, "Customer_6", "Perth", "Code3"),
    ]

    df = spark.createDataFrame(data= test_data, schema = silver_customer.schema)

    test_df = silver_customer.unionByName(df, allowMissingColumns=True)


    # STEP 2: Act
    result = customer_business_transformation(customer=test_df)

    actual_number_of_rows = result.count()

    # STEP 3: Assert
    expected_number_of_rows = 4
    assert actual_number_of_rows == expected_number_of_rows




