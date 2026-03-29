import dlt

sales_rule={
    "rule1":"sales_id IS NOT NULL "
}

dlt.create_streaming_table(comment="This table contains the raw data from the bronze layer",
                            name="sales_stg",
                            expect_all_or_drop=sales_rule)

@dlt.append_flow(target="sales_stg")
def east_sales():
    df=spark.readStream.table('dlt_catalog.source.sales_east')
    return df

@dlt.append_flow(target="sales_stg")
def west_sales():
    df=spark.readStream.table('dlt_catalog.source.sales_west')
    return df

