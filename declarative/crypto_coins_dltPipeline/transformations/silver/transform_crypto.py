import dlt
from pyspark.sql.functions import initcap
@dlt.view(name="enriched_crypto")
def enriched_crypto():
    df=spark.readStream.table("crypto_dlt.dlt_schema.stg_crypto")
    df = df.withColumn("name", initcap("name"))
    return df

dlt.create_streaming_table(
    name="transformed_crypto"
)

dlt.create_auto_cdc_flow(
    source="enriched_crypto",
    target="transformed_crypto",
    sequence_by="ingestion_time",
    keys=["id"],
    stored_as_scd_type=1 # upsert
)