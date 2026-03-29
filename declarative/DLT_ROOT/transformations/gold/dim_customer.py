import dlt
from pyspark.sql import functions as f

dlt.create_streaming_table(
    name="dim_customer"
)

dlt.create_auto_cdc_flow(
    target='dim_customer',
    source='customer_enr_view',
    sequence_by='last_updated',
    keys=['customer_id'],
    stored_as_scd_type=2
)