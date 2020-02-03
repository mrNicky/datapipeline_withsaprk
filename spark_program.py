from pyspark.sql import SparkSession, SQLContext 

print("EXTRACT FROM csv")
if __name__ == "__main__":
	scSpark = SparkSession \
	.builder \
	.appName("reading csv") \
	.getOrCreate()

	data_file = 'supermarket_sales.csv'
	sData = scSpark.read.csv(data_file, header=True, sep=",").cache()
	extract = sData.where(sData["Total"] > 100)
	extract.show()
	print("Total Records of our data : {}".format(extract.count()))

	print("TRANSFORM")
	extract.registerTempTable("sales")
	output = scSpark.sql("SELECT * FROM sales")
	output.show()

	print("LOAD") 
	output.write.format("json").save("filtered.json") 
	print("SAVING IN JSON in file")
