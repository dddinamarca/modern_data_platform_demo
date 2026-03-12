df = spark.read.format("delta").load("lakehouse/bronze/sales")

clean = df.filter("price > 0")

clean.write.format("delta").mode("overwrite") \
    .save("lakehouse/silver/sales_clean")
