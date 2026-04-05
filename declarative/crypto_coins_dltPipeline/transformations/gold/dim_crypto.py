import dlt

dlt.create_streaming_table(
    name="scd2_coins"
)

dlt.create_auto_cdc_flow(
    target="scd2_coins",
    source="enriched_crypto",
    keys=["id"],
    stored_as_scd_type=2,
    sequence_by="ingestion_time"
)