import dlt

customer_rule={
    "rule1":"customer_id IS NOT NULL",
    "rule2":"customer_name IS NOT NULL"
}
dlt.create_streaming_table(
    name="customer_stg",
    expect_all_or_drop=customer_rule
)

@dlt.append_flow(
    target="customer_stg"
)
def customer_stg():
    df=spark.readStream.table("dlt_catalog.source.customers")
    return df