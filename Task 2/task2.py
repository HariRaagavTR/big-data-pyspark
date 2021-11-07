#!/usr/bin/env python3
import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import max

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

cityFilePath = sys.argv[1].strip()
globalFilePath = sys.argv[2].strip()

cityFile = sqlContext.read.csv(cityFilePath, header = True)
globalFile = sqlContext.read.csv(globalFilePath, header = True)

cityConverted = cityFile.withColumn("AverageTemperature", cityFile.AverageTemperature.cast('float'))
cityMaxTemp = cityConverted.groupby('Country', 'dt').agg(max("AverageTemperature").alias('MaxTemp'))

result = cityMaxTemp.alias('df1').join(
    globalFile.alias('df2'), 
    [cityMaxTemp.dt == globalFile.dt, cityMaxTemp.MaxTemp > globalFile.LandAverageTemperature], 
    how = 'inner'
).groupby("Country").count()

for row in sorted(result.collect()):
    print('%s\t%s' % (row[0], row[1]))