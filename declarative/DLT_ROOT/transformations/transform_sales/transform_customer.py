import dlt 
from pyspark.sql import functions as f

@dlt.view(name='customer_enr_view')
def customer_enr_view():
    df=spark.readStream.table("customer_stg")
    df = df.withColumn("customer_name", f.lower(f.col('customer_name')))
    return df
dlt.create_streaming_table(
    name="customer_tran"
)

dlt.create_auto_cdc_flow(
    target='customer_tran',
    source='customer_enr_view',
    keys=['customer_id'],
    sequence_by='last_updated',
    stored_as_scd_type=1
) 