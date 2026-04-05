import dlt

rules = {
    "rule1":"id IS NOT NULL",
    "rule2":"current_price IS NOT NULL",
    "rule3":"market_cap IS NOT NULL",
    "rule4":"last_updated IS NOT NULL"

}
dlt.create_streaming_table(
    name="stg_crypto",
    expect_all_or_drop=rules
)
@dlt.append_flow(target="stg_crypto")
def stg_crypto():
    df=spark.readStream.table("crypto_dlt.source.crypto_raw")
    return df