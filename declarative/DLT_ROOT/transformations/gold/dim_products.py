import dlt
from pyspark.sql import functions as f

dlt.create_streaming_table(
    name="dim_products"
)

dlt.create_auto_cdc_flow(
    target='dim_products',
    source='product_enr_view',
    sequence_by='last_updated',
    keys=['product_id'],
    stored_as_scd_type=2
)