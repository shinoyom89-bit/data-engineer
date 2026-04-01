import dlt 
from pyspark.sql.functions import *

@dlt.table(
    name="buisness_table"
)
def bv_table():
    fact_table=spark.read.table("fact_sales_scd2")
    dim_customer=spark.read.table("dim_customer")
    dim_product=spark.read.table("dim_products")
    
    df_join=fact_table.join(dim_customer,fact_table.customer_id==dim_customer.customer_id,"inner").join(dim_product,fact_table.product_id==dim_product.product_id)
    df_join=df_join.groupBy("category", "region")\
        .agg(sum("total_amount").alias("total_amount"))\
        .orderBy(desc("total_amount"))
                            

    df_join=df_join.select("region","category","total_amount") 
    return df_join

                            
