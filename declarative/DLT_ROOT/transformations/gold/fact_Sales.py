import dlt

dlt.create_streaming_table(
    name="fact_sales"
)

dlt.create_auto_cdc_flow(
    source="sales_enriched_view",
    keys=["sales_id"],
    target="fact_sales",
    stored_as_scd_type=1,
    sequence_by="sale_timestamp"
)
