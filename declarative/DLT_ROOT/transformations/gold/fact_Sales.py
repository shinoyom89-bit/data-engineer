import dlt

dlt.create_streaming_table(
    name="fact_sales_scd2"
)

dlt.create_auto_cdc_flow(
    source="sales_enriched_view",
    keys=["sales_id"],
    target="fact_sales_scd2",
    stored_as_scd_type=2,
    sequence_by="sale_timestamp"
)
