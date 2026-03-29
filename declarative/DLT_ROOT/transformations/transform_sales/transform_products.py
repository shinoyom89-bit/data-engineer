import dlt 
from pyspark.sql import functions as f

@dlt.view(name='product_enr_view')
def product_enr_view():
    df=spark.readStream.table("product_stg")
    df=df.withColumn("price",f.col('price').cast("int"))
    return df
dlt.create_streaming_table(
    name="products_tran"
)

dlt.create_auto_cdc_flow(
    target='products_tran',
    source='product_enr_view',
    keys=['product_id'],
    sequence_by='last_updated',
    stored_as_scd_type=1
) 