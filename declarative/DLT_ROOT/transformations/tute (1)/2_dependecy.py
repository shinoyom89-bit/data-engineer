# ''' creating the end to end basic flow pipeline '''
# ''' staging area'''
# import dlt
# from pyspark.sql.functions import as f
# @dlt.table(
#     name="staging_order"
# )
# def staging_order():

#     df=spark.readStream.table("dlt_catalog.source.orders")
#     return df

# ''' # creating another object on top of it ''' 
# @dlt.view(
#     name="order_transformed"
# )
# def order_transformed():
#     df = spark.readStream.table("staging_order")
#     df=df.withColumnRenamed("product",lower(f.('product')))
#     return df

# # creating aggregate
# @dlt.table(
#     name="transform_order"
# )
# def order_agg():
#     df = spark.readStream.table("order_transformed")
#     df=df.groupBy("customer_id").agg(sum("amount").alias("total_spends"))
#     return df





