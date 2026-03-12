df = spark.read.format("delta").load("lakehouse/silver/sales_clean")

metrics = df.groupBy("product").agg(sum("quantity").alias("units_sold"),sum(expr("price * quantity")).alias("revenue"))

metrics.write.format("delta").mode("overwrite").save("lakehouse/gold/product_metrics")
