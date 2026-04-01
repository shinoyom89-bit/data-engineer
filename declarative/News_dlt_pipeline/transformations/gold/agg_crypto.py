import dlt
from pyspark.sql.functions import col, when
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

@dlt.table(name="gold_crypto_summary")
def gold_crypto_summary():
    df = dlt.read("crypto_dlt.dlt_schema.transformed_crypto")

    window = Window.orderBy(col("market_cap").desc())

    df = df.withColumn("rank", row_number().over(window))

    df = df.withColumn(
        "price_category",
        when(col("current_price") > 1000, "high")
        .when(col("current_price") > 100, "medium")
        .otherwise("low")
    )

    df = df.filter(col("rank") <= 20)

    return df.select(
        "id",
        "name",
        "current_price",
        "market_cap",
        "rank",
        "price_category"
    )