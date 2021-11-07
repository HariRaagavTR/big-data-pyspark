#!/usr/bin/env python3
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import avg
import sys

sparkContext = SparkContext()
sqlContext = SQLContext(sparkContext)

country = sys.argv[1]
cityFilePath = sys.argv[2]

dataFrame = sqlContext.read.option("header", True).csv(cityFilePath)
dataFrame = dataFrame.filter(dataFrame.Country == country)

avgCityTemperatures = dataFrame.groupBy('City').agg(avg('AverageTemperature').alias('AvgTemp')).select('City', 'AvgTemp')

queryTable = dataFrame.alias('df1').join(
    avgCityTemperatures.alias('df2'), dataFrame.City == avgCityTemperatures.City, how = 'inner'
).select('df1.City', 'df1.AverageTemperature', 'df2.AvgTemp')

result = queryTable.filter(queryTable.AverageTemperature > queryTable.AvgTemp).groupBy('City').count()

for row in sorted(result.collect()):
    print('%s\t%s' % (row[0], row[1]))