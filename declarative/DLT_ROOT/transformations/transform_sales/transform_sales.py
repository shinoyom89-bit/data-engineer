import dlt
from pyspark.sql import functions as F

@dlt.view(name="sales_enriched_view")
def stg_transform():
    df = spark.readStream.table("sales_stg") # read from the bronze layer
    
    df = df.withColumn(
        "total_amount",
        (F.col("quantity") * F.col("amount")).cast("double")
    )
    
    return df


dlt.create_streaming_table(
    name="sales_enriched"
)
# if the change happend in the bronze the source table then upsert here and send to silver layer after transform
dlt.create_auto_cdc_flow( 
    source="sales_enriched_view", # we did not send the stg_layer_Data direclty to the stream table of transform cause Streaming_table=append_only behaviour 
    # so if the smae product id came with update in the price it will keep append that but we need to change the price so what we did is wee upsert then append to the silver 
    target="sales_enriched",
    keys=["sales_id"],
    sequence_by="sale_timestamp",
    stored_as_scd_type=1
)
