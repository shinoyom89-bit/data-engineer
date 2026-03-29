# # creating stream table
# import dlt
# @dlt.table(
#     name="first_stream_table"
# )
# def first_stream_table():
#     df=spark.readStream.table('dlt_catalog.source.orders')
#     return df

# # creating the view
# ''' Materialized view does not rewrite all rows; it incrementally updates the existing stored result using only new data, but the final table always shows the complete dataset.'''
# @dlt.materialized_view(
#             name = "first_mat_view"
# )
# def first_mat_view():
#     df=spark.read.table('dlt_catalog.source.orders')
#     return df

# # creating batch view
# @dlt.view(
#     name="first_batch_view"
# )
# def first_batch_view():
#     df=spark.read.table('dlt_catalog.source.orders')
#     return df

# @dlt.view(
#     name="first_stream_view"
# )
# def first_stream_view():
#     df=spark.readStream.table('dlt_catalog.source.orders')
#     return df