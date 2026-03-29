import dlt
# excpectation 
products_rule={
    "rule_1":' product_id IS NOT NULL',
    "rule2":"price>=0"
}
dlt.create_streaming_table(
    name="product_stg",
    expect_all_or_drop=products_rule
)

@dlt.append_flow(target="product_stg")
def load_product():
        df= spark.readStream.table('dlt_catalog.source.products')
        return df
    