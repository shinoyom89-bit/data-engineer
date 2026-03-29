import dlt
from pyspark.sql import functions as F

@dlt.view(name="sales_enriched_view")
def stg_transform():
    df = dlt.read_stream("sales_stg")
    
    df = df.withColumn(
        "total_amount",
        (F.col("quantity") * F.col("amount")).cast("double")
    )
    
    return df


dlt.create_streaming_table(
    name="sales_enriched"
)
dlt.create_auto_cdc_flow(
    source="sales_enriched_view",
    target="sales_enriched",
    keys=["sales_id"],
    sequence_by="sale_timestamp",
    stored_as_scd_type=1
)